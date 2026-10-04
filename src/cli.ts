import { Engine } from "./engine.js";
import { openDb } from "./db.js";
import { DrrError } from "./errors.js";

const USAGE = `用法: node dist/cli.js <命令> [参数]
  create-run [name]
  list-runs
  add-stream <runId> <streamId> <quantum>
  enqueue <runId> <streamId> <packetId> <length>
  advance <runId> [count]
  ring <runId>
  events <runId> [streamId]
  explain <runId> <visit>
  packet <runId> <streamId> <packetId>
  export <runId>
环境变量: DRR_DATA_DIR 指定数据目录（默认 ./data）
`;

function main(): void {
  const [cmd, ...args] = process.argv.slice(2);
  if (!cmd) {
    process.stdout.write(USAGE);
    process.exit(1);
  }
  const engine = new Engine(openDb());
  const out = (v: unknown): void => {
    process.stdout.write(JSON.stringify(v, null, 2) + "\n");
  };
  switch (cmd) {
    case "create-run":
      return out(engine.createRun(args[0]));
    case "list-runs":
      return out(engine.listRuns());
    case "add-stream":
      engine.registerStream(Number(args[0]), args[1], Number(args[2]));
      return out({ ok: true });
    case "enqueue":
      engine.enqueue(Number(args[0]), args[1], args[2], Number(args[3]));
      return out({ ok: true });
    case "advance":
      return out(engine.advance(Number(args[0]), args[1] ? Number(args[1]) : 1));
    case "ring":
      return out(engine.ring(Number(args[0])));
    case "events":
      return out(engine.events(Number(args[0]), args[1]));
    case "explain":
      return out(engine.explain(Number(args[0]), Number(args[1])));
    case "packet":
      return out(engine.packetStatus(Number(args[0]), args[1], args[2]));
    case "export":
      return out(engine.exportRun(Number(args[0])));
    default:
      process.stderr.write(`未知命令 ${cmd}\n${USAGE}`);
      process.exit(1);
  }
}

try {
  main();
} catch (err) {
  if (err instanceof DrrError) {
    process.stderr.write(`错误 [${err.code}] ${err.message}\n`);
    process.exit(2);
  }
  throw err;
}
