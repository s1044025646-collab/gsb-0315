import { test } from "node:test";
import assert from "node:assert/strict";
import { openDb } from "../src/db";
import { DrrEngine } from "../src/engine";

test("确定性：同一事件序列产生相同账目", () => {
  const build = () => {
    const e = new DrrEngine(openDb(":memory:"));
    const run = e.createRun("t");
    e.addStream(run.id, "A", 2);
    e.addStream(run.id, "B", 3);
    e.enqueue(run.id, "A", "A1", 5);
    e.enqueue(run.id, "B", "B1", 4);
    e.enqueue(run.id, "A", "A2", 1);
    e.advance(run.id, 10);
    const x = e.exportRun(run.id);
    return { streams: x.streams, packets: x.packets, events: x.events };
  };
  assert.deepEqual(build(), build());
});

test("长期字节份额趋近 quantum 比（始终有积压）", () => {
  const e = new DrrEngine(openDb(":memory:"));
  const run = e.createRun("t");
  e.addStream(run.id, "A", 1);
  e.addStream(run.id, "B", 2);
  for (let i = 0; i < 2000; i++) {
    e.enqueue(run.id, "A", `A${i}`, 1);
    e.enqueue(run.id, "B", `B${i}`, 3);
  }
  e.advance(run.id, 1500);
  const h = e.history(run.id);
  const a = h.streams[0].servedBytes, b = h.streams[1].servedBytes;
  const ratio = b / a;
  assert.ok(ratio > 1.7 && ratio < 2.3, `份额比 ${ratio} 偏离 2 过多`);
});
