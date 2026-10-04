import { openDb } from "./db";
import { DrrEngine } from "./engine";
import { startServer } from "./api";
import { DrrError } from "./config";

const USAGE = `
DRR 字节公平队列模拟器 CLI
用法: node dist/src/cli.js <命令> [参数]

  serve [--port N]                        启动 HTTP API（默认自动选择空闲端口）
  create-run [name]                       创建运行
  runs                                    列出运行
  add-stream <runId> <streamId> <quantum> 登记流（运行推进后冻结）
  enqueue <runId> <streamId> <packetId> <length>  入队数据包
  advance <runId> [count]                 推进一次或多次访问
  ring <runId>                            查看活动环与当前指针
  events <runId> [streamId]               查看访问历史
  explain <runId> <seq>                   解释某次访问事件
  packet <runId> <packetId>               查询包状态
  history <runId>                         每流统计（服务字节/积压/守恒校验）
  export <runId>                          导出运行完整 JSON
  demo                                    运行内置演示
`;

function print(x: unknown): void {
  console.log(typeof x === "string" ? x : JSON.stringify(x, null, 2));
}

export async function main(argv: string[]): Promise<void> {
  const [cmd, ...args] = argv;
  if (!cmd || cmd === "help" || cmd === "--help") {
    console.log(USAGE);
    return;
  }
  const engine = new DrrEngine(openDb());
  switch (cmd) {
    case "serve": {
      const idx = args.indexOf("--port");
      const port = idx >= 0 ? Number(args[idx + 1]) : 0;
      if (idx >= 0 && (!Number.isInteger(port) || port < 0 || port > 65535)) {
        throw new DrrError("INVALID_ARGUMENT", "--port 必须是 0..65535 的整数（0 表示自动选择空闲端口）");
      }
      const { port: actual } = await startServer(engine, port);
      console.log(`DRR API 已启动: http://127.0.0.1:${actual}`);
      return;
    }
    case "create-run": print(engine.createRun(args[0] ?? "")); return;
    case "runs": print(engine.listRuns()); return;
    case "add-stream": print(engine.addStream(Number(args[0]), args[1], Number(args[2]))); return;
    case "enqueue": print(engine.enqueue(Number(args[0]), args[1], args[2], Number(args[3]))); return;
    case "advance": {
      const events = engine.advance(Number(args[0]), args[1] !== undefined ? Number(args[1]) : 1);
      if (events.length === 0) console.log("活动环为空，无可服务流");
      else print(events);
      return;
    }
    case "ring": print(engine.getRing(Number(args[0]))); return;
    case "events": print(engine.listEvents(Number(args[0]), args[1])); return;
    case "explain": print(engine.explainEvent(Number(args[0]), Number(args[1]))); return;
    case "packet": print(engine.getPacket(Number(args[0]), args[1])); return;
    case "history": print(engine.history(Number(args[0]))); return;
    case "export": print(engine.exportRun(Number(args[0]))); return;
    case "demo": {
      const { runDemo } = await import("./demo");
      runDemo(engine);
      return;
    }
    default:
      console.error(`未知命令: ${cmd}`);
      console.log(USAGE);
      process.exitCode = 2;
  }
}

if (require.main === module) {
  main(process.argv.slice(2)).catch((e) => {
    if (e instanceof DrrError) {
      console.error(`错误 [${e.code}]: ${e.message}`);
      process.exitCode = 1;
    } else {
      console.error(e);
      process.exitCode = 1;
    }
  });
}
