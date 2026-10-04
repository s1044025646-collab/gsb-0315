import { DrrEngine } from "./engine";

function showEvents(engine: DrrEngine, runId: number, fromSeq: number): void {
  for (const ev of engine.listEvents(runId)) {
    if (ev.seq < fromSeq) continue;
    const sent = ev.sentPackets.length
      ? ev.sentPackets.map(p => `${p.packetKey}(${p.length}B)`).join(",")
      : "无";
    const blocked = ev.headBlocked ? ` 队首被阻:${ev.headBlocked.packetKey}(${ev.headBlocked.length}B)` : "";
    console.log(
      `  #${ev.seq} 流=${ev.streamKey} 额度 ${ev.deficitBefore}+${ev.added} -> ${ev.deficitAfter}` +
      ` 发出:[${sent}] ${ev.sentBytes}B${blocked}${ev.queueEmptied ? " [清空并移出环]" : ""}` +
      ` 下一流=${ev.nextStreamKey ?? "无"}`
    );
  }
}

export function runDemo(engine: DrrEngine): void {
  console.log("=== 演示 1：quantum=2 的流发送长度 3 的包（亏欠结转） ===");
  const r1 = engine.createRun("demo-1");
  engine.addStream(r1.id, "A", 2);
  engine.enqueue(r1.id, "A", "A-p1", 3);
  engine.advance(r1.id, 1);
  console.log("第一次访问：额度 0+2=2 < 3，无法发送，保留 2 字节额度");
  engine.advance(r1.id, 1);
  console.log("第二次访问：额度 2+2=4 >= 3，发送后剩余 1，队列清空额度归零");
  showEvents(engine, r1.id, 0);

  console.log("\n=== 演示 2：两个流不同 quantum 与包长的交错访问 ===");
  const r2 = engine.createRun("demo-2");
  engine.addStream(r2.id, "A", 3);
  engine.addStream(r2.id, "B", 1);
  engine.enqueue(r2.id, "A", "A-p1", 5);
  engine.enqueue(r2.id, "A", "A-p2", 2);
  engine.enqueue(r2.id, "B", "B-p1", 1);
  engine.enqueue(r2.id, "B", "B-p2", 4);
  engine.advance(r2.id, 8);
  showEvents(engine, r2.id, 0);

  console.log("\n=== 演示 3：流清空后重新入队，在环尾重新激活（不继承旧额度） ===");
  const r3 = engine.createRun("demo-3");
  engine.addStream(r3.id, "A", 4);
  engine.addStream(r3.id, "B", 4);
  engine.enqueue(r3.id, "A", "A-p1", 1);
  engine.enqueue(r3.id, "B", "B-p1", 10);
  engine.advance(r3.id, 2); // A 发完 1B 清空出环；B 发不出 10B 但保留额度
  console.log("A 清空出环后重新入队 A-p2(2B)，排在环尾，额度从 0 重新开始：");
  engine.enqueue(r3.id, "A", "A-p2", 2);
  engine.advance(r3.id, 2);
  showEvents(engine, r3.id, 0);

  console.log("\n=== 长期字节份额观察（A:q=1 小包，B:q=2 大包，始终有积压） ===");
  const r4 = engine.createRun("demo-4");
  engine.addStream(r4.id, "A", 1);
  engine.addStream(r4.id, "B", 2);
  for (let i = 0; i < 400; i++) {
    engine.enqueue(r4.id, "A", `A-p${i}`, 1);
    engine.enqueue(r4.id, "B", `B-p${i}`, 3);
  }
  engine.advance(r4.id, 600);
  const h = engine.history(r4.id);
  for (const s of h.streams) {
    console.log(`  流 ${s.streamKey}: quantum=${s.quantum} 已服务=${s.servedBytes}B 积压=${s.backlogBytes}B`);
  }
  const a = h.streams[0].servedBytes, b = h.streams[1].servedBytes;
  console.log(`  字节份额 A:B = ${a}:${b}（理想约 1:2）。短序列偏差来自包长粒度与队首阻塞，`);
  console.log("  这是字节公平而非包数公平；不能宣称任意有限时间窗内达到精确带宽比例。");
}
