import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { Engine } from "../src/engine.js";
import { openDb } from "../src/db.js";
import { DrrError } from "../src/errors.js";

function memEngine(): Engine {
  return new Engine(openDb(":memory:"));
}

function setup(run: { e: Engine; id: number }, streams: [string, number][], packets: [string, string, number][]): void {
  for (const [sid, q] of streams) run.e.registerStream(run.id, sid, q);
  for (const [sid, pid, len] of packets) run.e.enqueue(run.id, sid, pid, len);
}

test("quantum=2 长度=3 的包跨轮结转后发送", () => {
  const e = memEngine();
  const id = e.createRun("t").runId;
  setup({ e, id }, [["A", 2]], [["A", "p1", 3]]);
  const v1 = e.advance(id, 1)[0];
  assert.equal(v1.sentBytes, 0);
  assert.equal(v1.deficitAfter, 2);
  assert.equal(v1.headBlocked, "p1");
  const v2 = e.advance(id, 1)[0];
  assert.equal(v2.deficitBefore, 2);
  assert.deepEqual(v2.sentPackets, [{ packetId: "p1", length: 3 }]);
  assert.equal(v2.deficitAfter, 0); // 队列变空，额度归零并移出环
  assert.equal(v2.streamEmptied, true);
  assert.throws(() => e.advance(id, 1), (err: unknown) => err instanceof DrrError && err.code === "NO_ACTIVE_STREAMS");
});

test("单流环连续发多包，剩余额度不足时保留", () => {
  const e = memEngine();
  const id = e.createRun("t").runId;
  setup({ e, id }, [["A", 5]], [["A", "p1", 2], ["A", "p2", 2], ["A", "p3", 3]]);
  const v1 = e.advance(id, 1)[0];
  assert.deepEqual(v1.sentPackets.map((p) => p.packetId), ["p1", "p2"]);
  assert.equal(v1.deficitAfter, 1);
  assert.equal(v1.headBlocked, "p3");
  const v2 = e.advance(id, 1)[0];
  assert.deepEqual(v2.sentPackets.map((p) => p.packetId), ["p3"]);
  assert.equal(v2.streamEmptied, true);
});

test("单流环：指针始终回到自身，不重复加额度", () => {
  const e = memEngine();
  const id = e.createRun("t").runId;
  setup({ e, id }, [["A", 2]], [["A", "p1", 100]]);
  const events = e.advance(id, 3);
  assert.deepEqual(events.map((v) => v.streamId), ["A", "A", "A"]);
  assert.deepEqual(events.map((v) => v.deficitBefore), [0, 2, 4]);
  assert.deepEqual(events.map((v) => v.nextStream), ["A", "A", "A"]);
});

test("环头尾移除：中间流清空后指针正确前进", () => {
  const e = memEngine();
  const id = e.createRun("t").runId;
  setup({ e, id }, [["A", 1], ["B", 1], ["C", 1]], [["B", "b1", 1]]);
  // 只有 B 有包；A、C 入队才会激活，这里先只激活 B
  let v = e.advance(id, 1)[0];
  assert.equal(v.streamId, "B");
  assert.equal(v.streamEmptied, true);
  assert.equal(v.nextStream, null);
  // 现在激活 A、B、C，顺序由激活事件决定
  e.enqueue(id, "A", "a1", 5);
  e.enqueue(id, "C", "c1", 5);
  e.enqueue(id, "B", "b2", 5);
  const seq = e.advance(id, 6).map((x) => x.streamId);
  assert.deepEqual(seq, ["A", "C", "B", "A", "C", "B"]);
});

test("入队已活跃流不改变环位置", () => {
  const e = memEngine();
  const id = e.createRun("t").runId;
  setup({ e, id }, [["A", 1], ["B", 1]], [["A", "a1", 1], ["B", "b1", 1], ["B", "b2", 1]]);
  e.enqueue(id, "A", "a2", 1); // A 已活跃
  const seq = e.advance(id, 4).map((x) => x.streamId);
  assert.deepEqual(seq, ["A", "B", "A", "B"]);
});

test("清空再激活：不继承旧额度，挂到环尾", () => {
  const e = memEngine();
  const id = e.createRun("t").runId;
  setup({ e, id }, [["A", 5], ["B", 5]], [["A", "a1", 1], ["B", "b1", 10]]);
  e.advance(id, 1); // A 发完清空，剩余 4 额度被清零
  e.enqueue(id, "A", "a2", 1); // 重新激活
  e.advance(id, 1); // B
  const v = e.advance(id, 1)[0]; // A 重新激活后首次访问
  assert.equal(v.streamId, "A");
  assert.equal(v.deficitBefore, 0); // 未继承旧的 4
  assert.equal(v.quantumAdded, 5);
});

test("字节守恒：入队字节 = 已服务 + 积压，且从不越过队首", () => {
  const e = memEngine();
  const id = e.createRun("t").runId;
  const lens = [3, 1, 4, 1, 5, 9, 2, 6];
  setup({ e, id }, [["A", 4], ["B", 2]], []);
  let totalEnq = 0;
  lens.forEach((len, i) => {
    const sid = i % 2 === 0 ? "A" : "B";
    e.enqueue(id, sid, `p${i}`, len);
    totalEnq += len;
  });
  const events = e.advance(id, 12);
  // FIFO 检查：每流 sent_visit 递增顺序即 seq 顺序
  const exported = e.exportRun(id) as { packets: Array<{ stream_id: string; seq: number; sent_visit: number | null; status: string }> };
  for (const sid of ["A", "B"]) {
    const ps = exported.packets.filter((p) => p.stream_id === sid).sort((a, b) => a.seq - b.seq);
    let lastVisit = -1;
    for (const p of ps) {
      if (p.status === "sent") {
        assert.ok(p.sent_visit! >= lastVisit, "不允许越过队首发送");
        lastVisit = p.sent_visit!;
      } else {
        // 未发送的包之后不允许有已发送的包
        assert.ok(ps.slice(ps.indexOf(p)).every((x) => x.status === "queued"));
      }
    }
  }
  const ring = e.ring(id);
  const streams = ring.streams as Array<{ servedBytes: number; backlogBytes: number }>;
  const served = streams.reduce((s, x) => s + x.servedBytes, 0);
  const backlog = streams.reduce((s, x) => s + x.backlogBytes, 0);
  assert.equal(served + backlog, totalEnq);
  assert.equal(served, events.reduce((s, x) => s + x.sentBytes, 0));
});

test("失败回滚：重复包号、未知流、零长度不消耗额度不留痕迹", () => {
  const e = memEngine();
  const id = e.createRun("t").runId;
  setup({ e, id }, [["A", 2]], [["A", "p1", 3]]);
  assert.throws(() => e.enqueue(id, "A", "p1", 5), (x: unknown) => (x as DrrError).code === "DUPLICATE_PACKET");
  assert.throws(() => e.enqueue(id, "ZZZ", "p9", 1), (x: unknown) => (x as DrrError).code === "UNKNOWN_STREAM");
  assert.throws(() => e.enqueue(id, "A", "p2", 0), (x: unknown) => (x as DrrError).code === "INVALID_ARGUMENT");
  assert.throws(() => e.registerStream(id, "B", -1), (x: unknown) => (x as DrrError).code === "INVALID_ARGUMENT");
  assert.throws(() => e.advance(id, 0), (x: unknown) => (x as DrrError).code === "INVALID_ARGUMENT");
  const ring = e.ring(id);
  const a = (ring.streams as Array<{ streamId: string; backlogBytes: number; backlogPackets: number }>)[0];
  assert.equal(a.backlogBytes, 3);
  assert.equal(a.backlogPackets, 1);
  const v = e.advance(id, 1)[0];
  assert.equal(v.deficitBefore, 0); // 失败未消耗/污染额度
});

test("重启续算：指针与额度持久化，不从第一个流重新开始", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "drr-"));
  const dbFile = path.join(dir, "test.db");
  let e = new Engine(openDb(dbFile));
  const id = e.createRun("t").runId;
  setup({ e, id }, [["A", 1], ["B", 1], ["C", 1]], [["A", "a", 9], ["B", "b", 9], ["C", "c", 9]]);
  e.advance(id, 1); // 服务 A，指针应指向 B
  e.close();
  e = new Engine(openDb(dbFile)); // 模拟重启
  const v = e.advance(id, 1)[0];
  assert.equal(v.streamId, "B");
  assert.equal(v.deficitBefore, 0);
  const v2 = e.advance(id, 1)[0];
  assert.equal(v2.streamId, "C");
  e.close();
  fs.rmSync(dir, { recursive: true, force: true });
});

test("流参数冻结：开始调度后注册新流被拒绝", () => {
  const e = memEngine();
  const id = e.createRun("t").runId;
  setup({ e, id }, [["A", 1]], [["A", "a", 1]]);
  e.advance(id, 1);
  assert.throws(() => e.registerStream(id, "B", 1), (x: unknown) => (x as DrrError).code === "STREAMS_FROZEN");
});

test("长期字节份额：持续积压下趋近 quantum 比例", () => {
  const e = memEngine();
  const id = e.createRun("t").runId;
  setup({ e, id }, [["A", 3], ["B", 1]], []);
  for (let i = 0; i < 2000; i++) {
    e.enqueue(id, "A", `a${i}`, 2);
    e.enqueue(id, "B", `b${i}`, 2);
  }
  e.advance(id, 2000);
  const ring = e.ring(id);
  const streams = ring.streams as Array<{ streamId: string; servedBytes: number }>;
  const a = streams.find((s) => s.streamId === "A")!.servedBytes;
  const b = streams.find((s) => s.streamId === "B")!.servedBytes;
  const ratio = a / (a + b);
  // 理想比例 3:1 => 0.75；包长为 2 导致量化偏差，允许 ±0.03
  assert.ok(Math.abs(ratio - 0.75) < 0.03, `比例 ${ratio} 偏离过大`);
});

test("多运行命名空间独立", () => {
  const e = memEngine();
  const r1 = e.createRun("r1").runId;
  const r2 = e.createRun("r2").runId;
  setup({ e, id: r1 }, [["A", 1]], [["A", "p", 1]]);
  setup({ e, id: r2 }, [["A", 1]], [["A", "p", 1]]); // 同名流与包不冲突
  assert.equal(e.advance(r1, 1)[0].streamId, "A");
  assert.equal(e.advance(r2, 1)[0].streamId, "A");
});

test("explain 生成可读解释", () => {
  const e = memEngine();
  const id = e.createRun("t").runId;
  setup({ e, id }, [["A", 2]], [["A", "p1", 3]]);
  e.advance(id, 1);
  const x = e.explain(id, 1) as { explanation: string };
  assert.match(x.explanation, /额度 0 \+ quantum 2/);
  assert.match(x.explanation, /队首 p1/);
});


