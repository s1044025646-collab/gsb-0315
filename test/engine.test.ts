import { test } from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { openDb } from "../src/db";
import { DrrEngine } from "../src/engine";
import { DrrError } from "../src/config";

function mem(): DrrEngine {
  return new DrrEngine(openDb(":memory:"));
}

test("quantum=2 发送长度 3 的包：第一次保留额度，第二次发送", () => {
  const e = mem();
  const run = e.createRun("t");
  e.addStream(run.id, "A", 2);
  e.enqueue(run.id, "A", "p1", 3);
  const [ev1] = e.advance(run.id, 1);
  assert.equal(ev1.deficitBefore, 0);
  assert.equal(ev1.added, 2);
  assert.equal(ev1.sentBytes, 0);
  assert.equal(ev1.deficitAfter, 2); // 保留 2 字节额度
  assert.deepEqual(ev1.headBlocked, { packetKey: "p1", length: 3 });
  const [ev2] = e.advance(run.id, 1);
  assert.equal(ev2.deficitBefore, 2);
  assert.equal(ev2.deficitAfter, 0); // 4-3=1 但队列清空，额度归零
  assert.equal(ev2.sentBytes, 3);
  assert.equal(ev2.queueEmptied, true);
  assert.equal(ev2.nextStreamKey, null);
});

test("单流环：下一流是自己", () => {
  const e = mem();
  const run = e.createRun("t");
  e.addStream(run.id, "A", 1);
  e.enqueue(run.id, "A", "p1", 10);
  const [ev] = e.advance(run.id, 1);
  assert.equal(ev.nextStreamKey, "A");
});

test("一次访问连续发送多个小包", () => {
  const e = mem();
  const run = e.createRun("t");
  e.addStream(run.id, "A", 10);
  e.enqueue(run.id, "A", "p1", 3);
  e.enqueue(run.id, "A", "p2", 4);
  e.enqueue(run.id, "A", "p3", 5);
  const [ev] = e.advance(run.id, 1);
  assert.deepEqual(ev.sentPackets.map(p => p.packetKey), ["p1", "p2"]);
  assert.equal(ev.deficitAfter, 3); // 10-3-4，p3=5 放不下
  assert.deepEqual(ev.headBlocked, { packetKey: "p3", length: 5 });
});

test("不跳过队首大包", () => {
  const e = mem();
  const run = e.createRun("t");
  e.addStream(run.id, "A", 2);
  e.enqueue(run.id, "A", "big", 100);
  e.enqueue(run.id, "A", "small", 1);
  const [ev] = e.advance(run.id, 1);
  assert.equal(ev.sentBytes, 0);
  assert.equal(e.getPacket(run.id, "small").status, "queued");
});

test("交错访问顺序与人工访问表一致", () => {
  const e = mem();
  const run = e.createRun("t");
  e.addStream(run.id, "A", 3);
  e.addStream(run.id, "B", 1);
  e.enqueue(run.id, "A", "A1", 5);
  e.enqueue(run.id, "B", "B1", 1);
  e.enqueue(run.id, "B", "B2", 1);
  // 人工表:
  // #0 A: 0+3=3 <5 发不出, 余3; ->B
  // #1 B: 0+1=1 发B1, 余0, 队列还有B2 不出环; ->A
  // #2 A: 3+3=6 发A1(5), 余1, 清空出环; ->B
  // #3 B: 0+1=1 发B2, 清空出环; ->null
  const evs = e.advance(run.id, 4);
  assert.equal(evs.length, 4);
  assert.deepEqual(evs.map(x => x.streamKey), ["A", "B", "A", "B"]);
  assert.equal(evs[0].deficitAfter, 3);
  assert.equal(evs[1].sentBytes, 1);
  assert.equal(evs[2].deficitAfter, 0);
  assert.equal(evs[2].queueEmptied, true);
  assert.equal(evs[3].nextStreamKey, null);
  // 环空后再推进不产生事件
  assert.equal(e.advance(run.id, 3).length, 0);
});

test("清空后重新激活：排环尾、额度归零", () => {
  const e = mem();
  const run = e.createRun("t");
  e.addStream(run.id, "A", 4);
  e.addStream(run.id, "B", 4);
  e.enqueue(run.id, "A", "A1", 1);
  e.enqueue(run.id, "B", "B1", 100);
  e.advance(run.id, 1); // A 发完出环
  e.enqueue(run.id, "A", "A2", 2); // 重新激活，环尾
  const ring = e.getRing(run.id);
  assert.deepEqual(ring.ring.map((r: any) => r.streamKey), ["B", "A"]);
  assert.equal(ring.ring[1].deficit, 0); // 不继承旧额度
  const [ev] = e.advance(run.id, 1); // 指针仍指向 B
  assert.equal(ev.streamKey, "B");
});

test("入队活跃流不改变环中位置", () => {
  const e = mem();
  const run = e.createRun("t");
  e.addStream(run.id, "A", 1);
  e.addStream(run.id, "B", 1);
  e.enqueue(run.id, "A", "A1", 5);
  e.enqueue(run.id, "B", "B1", 5);
  e.enqueue(run.id, "A", "A2", 5); // A 已活跃
  const ring = e.getRing(run.id);
  assert.deepEqual(ring.ring.map((r: any) => r.streamKey), ["A", "B"]);
});

test("移除头节点与尾节点后指针正确", () => {
  const e = mem();
  const run = e.createRun("t");
  e.addStream(run.id, "A", 1);
  e.addStream(run.id, "B", 1);
  e.addStream(run.id, "C", 1);
  e.enqueue(run.id, "A", "A1", 1);
  e.enqueue(run.id, "B", "B1", 5);
  e.enqueue(run.id, "C", "C1", 1);
  const [ev0] = e.advance(run.id, 1); // A 发完出环（移除头）
  assert.equal(ev0.nextStreamKey, "B");
  // 推进到 C 发完出环（移除尾）
  e.advance(run.id, 1); // B
  const evs = e.advance(run.id, 2); // C, B
  assert.equal(evs[0].streamKey, "C");
  assert.equal(evs[0].queueEmptied, true);
  assert.equal(evs[0].nextStreamKey, "B"); // 环绕回
  assert.equal(evs[1].streamKey, "B");
});

test("空环推进返回空，不产生事件", () => {
  const e = mem();
  const run = e.createRun("t");
  assert.equal(e.advance(run.id, 5).length, 0);
  assert.equal(e.listEvents(run.id).length, 0);
});

test("重复包编号、未知流、零长度、非法参数被拒绝", () => {
  const e = mem();
  const run = e.createRun("t");
  e.addStream(run.id, "A", 2);
  e.enqueue(run.id, "A", "p1", 1);
  assert.throws(() => e.enqueue(run.id, "A", "p1", 1), (err: any) => err.code === "DUPLICATE_PACKET");
  assert.throws(() => e.enqueue(run.id, "ZZ", "p2", 1), (err: any) => err.code === "STREAM_NOT_FOUND");
  assert.throws(() => e.enqueue(run.id, "A", "p3", 0), (err: any) => err.code === "INVALID_ARGUMENT");
  assert.throws(() => e.enqueue(run.id, "A", "p4", -5), (err: any) => err.code === "INVALID_ARGUMENT");
  assert.throws(() => e.enqueue(run.id, "A", "p5", 1.5), (err: any) => err.code === "INVALID_ARGUMENT");
  assert.throws(() => e.addStream(run.id, "A", 2), (err: any) => err.code === "DUPLICATE_STREAM");
  assert.throws(() => e.addStream(run.id, "B", 0), (err: any) => err.code === "INVALID_ARGUMENT");
  assert.throws(() => e.advance(9999, 1), (err: any) => err.code === "RUN_NOT_FOUND");
  assert.throws(() => e.advance(run.id, 0), (err: any) => err.code === "INVALID_ARGUMENT");
});

test("失败操作不消耗额度、不留已发送标记", () => {
  const e = mem();
  const run = e.createRun("t");
  e.addStream(run.id, "A", 2);
  e.enqueue(run.id, "A", "p1", 3);
  e.advance(run.id, 1); // 积累 2 额度
  assert.throws(() => e.enqueue(run.id, "A", "p1", 1)); // 重复，失败
  const ring = e.getRing(run.id);
  assert.equal(ring.ring[0].deficit, 2); // 额度未被消耗
  assert.equal(e.getPacket(run.id, "p1").status, "queued");
});

test("运行推进后冻结流参数", () => {
  const e = mem();
  const run = e.createRun("t");
  e.addStream(run.id, "A", 2);
  e.enqueue(run.id, "A", "p1", 1);
  e.advance(run.id, 1);
  assert.throws(() => e.addStream(run.id, "B", 1), (err: any) => err.code === "RUN_SEALED");
});

test("每流守恒：入队字节 = 已服务 + 积压", () => {
  const e = mem();
  const run = e.createRun("t");
  e.addStream(run.id, "A", 3);
  e.addStream(run.id, "B", 7);
  let seed = 42;
  const rnd = () => (seed = (seed * 1103515245 + 12345) & 0x7fffffff);
  for (let i = 0; i < 50; i++) {
    e.enqueue(run.id, "A", `A${i}`, 1 + (rnd() % 10));
    e.enqueue(run.id, "B", `B${i}`, 1 + (rnd() % 10));
  }
  e.advance(run.id, 80);
  const h = e.history(run.id);
  for (const s of h.streams) assert.ok(s.conserved, `流 ${s.streamKey} 不守恒`);
});

test("事件账目：从不越过队首", () => {
  const e = mem();
  const run = e.createRun("t");
  e.addStream(run.id, "A", 4);
  const lens = [3, 9, 2, 7, 1, 5];
  lens.forEach((l, i) => e.enqueue(run.id, "A", `p${i}`, l));
  e.advance(run.id, 20);
  const sentOrder = e.listEvents(run.id).flatMap(ev => ev.sentPackets.map(p => p.packetKey));
  // 发送顺序必须是入队顺序的前缀（FIFO 不被跳过）
  const enqOrder = lens.map((_, i) => `p${i}`);
  assert.deepEqual(sentOrder, enqOrder.slice(0, sentOrder.length));
});

test("重启后续算：指针与额度持久化", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "drr-"));
  const dbPath = path.join(dir, "t.db");
  let e = new DrrEngine(openDb(dbPath));
  const run = e.createRun("t");
  e.addStream(run.id, "A", 2);
  e.addStream(run.id, "B", 2);
  e.enqueue(run.id, "A", "A1", 3);
  e.enqueue(run.id, "B", "B1", 3);
  e.advance(run.id, 1); // 访问 A，指针应指向 B
  // 模拟重启
  e = new DrrEngine(openDb(dbPath));
  const [ev] = e.advance(run.id, 1);
  assert.equal(ev.streamKey, "B"); // 不重新从第一个流开始
  assert.equal(ev.deficitBefore, 0);
  const [evA] = e.advance(run.id, 1);
  assert.equal(evA.streamKey, "A");
  assert.equal(evA.deficitBefore, 2); // 重启前积累的额度仍在
});

test("多运行命名空间独立", () => {
  const e = mem();
  const r1 = e.createRun("r1");
  const r2 = e.createRun("r2");
  e.addStream(r1.id, "A", 1);
  e.addStream(r2.id, "A", 1); // 同名流不同运行，允许
  e.enqueue(r1.id, "A", "p1", 1);
  e.enqueue(r2.id, "A", "p1", 1); // 同名包不同运行，允许
  assert.equal(e.getPacket(r1.id, "p1").status, "queued");
  assert.equal(e.getPacket(r2.id, "p1").status, "queued");
});

test("事件解释与包状态查询", () => {
  const e = mem();
  const run = e.createRun("t");
  e.addStream(run.id, "A", 2);
  e.enqueue(run.id, "A", "p1", 3);
  e.advance(run.id, 1);
  const text = e.explainEvent(run.id, 0);
  assert.match(text, /访问流 'A'/);
  assert.match(text, /p1/);
  assert.equal(e.getPacket(run.id, "p1").status, "queued");
  e.advance(run.id, 1);
  assert.equal(e.getPacket(run.id, "p1").status, "sent");
  assert.throws(() => e.explainEvent(run.id, 99), (err: any) => err.code === "EVENT_NOT_FOUND");
});
