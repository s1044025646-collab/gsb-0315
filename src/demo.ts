import { Engine } from "./engine.js";
import { openDb } from "./db.js";

function show(engine: Engine, runId: number, label: string): void {
  const ev = engine.advance(runId, 1)[0];
  console.log(
    `${label} | 访问#${ev.visit} 流=${ev.streamId} 额度 ${ev.deficitBefore}+${ev.quantumAdded}` +
      ` 发出=[${ev.sentPackets.map((p) => `${p.packetId}:${p.length}B`).join(",")}]` +
      ` 余=${ev.deficitAfter} 阻塞队首=${ev.headBlocked ?? "-"} 下一流=${ev.nextStream ?? "(空)"}`
  );
}

const engine = new Engine(openDb(":memory:"));

console.log("=== 演示 1: quantum=2 的流发送长度 3 的包（跨轮结转） ===");
let run = engine.createRun("demo1").runId;
engine.registerStream(run, "A", 2);
engine.enqueue(run, "A", "p1", 3);
show(engine, run, "第一次访问无法发送，保留 2 字节额度");
show(engine, run, "第二次累加到 4，发送 3 字节包");

console.log("\n=== 演示 2: 两个流不同 quantum 与包长的交错访问 ===");
run = engine.createRun("demo2").runId;
engine.registerStream(run, "A", 3);
engine.registerStream(run, "B", 1);
engine.enqueue(run, "A", "a1", 2);
engine.enqueue(run, "A", "a2", 4);
engine.enqueue(run, "B", "b1", 1);
engine.enqueue(run, "B", "b2", 1);
engine.enqueue(run, "B", "b3", 1);
for (let i = 0; i < 5; i++) show(engine, run, `交错#${i + 1}`);

console.log("\n=== 演示 3: 流清空后移出环，再入队重新激活（不继承旧额度） ===");
run = engine.createRun("demo3").runId;
engine.registerStream(run, "A", 5);
engine.registerStream(run, "B", 5);
engine.enqueue(run, "A", "a1", 1);
engine.enqueue(run, "B", "b1", 10);
show(engine, run, "A 发完即清空移出环");
show(engine, run, "B 额度不足保留");
engine.enqueue(run, "A", "a2", 1);
console.log("A 重新入队 -> 环尾激活, 额度从 0 开始");
show(engine, run, "B 继续累加");
show(engine, run, "A 重新激活后首次访问");
console.log("\n活动环:", JSON.stringify(engine.ring(run)));
