import http from "node:http";
import { Engine } from "./engine.js";
import { openDb } from "./db.js";
import { DrrError } from "./errors.js";

function send(res: http.ServerResponse, status: number, body: unknown): void {
  const json = JSON.stringify(body, null, 2);
  res.writeHead(status, { "Content-Type": "application/json; charset=utf-8" });
  res.end(json);
}

async function readBody(req: http.IncomingMessage): Promise<Record<string, unknown>> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk as Buffer);
  if (chunks.length === 0) return {};
  const text = Buffer.concat(chunks).toString("utf8");
  try {
    return JSON.parse(text) as Record<string, unknown>;
  } catch {
    throw new DrrError("INVALID_ARGUMENT", "请求体不是合法 JSON");
  }
}

export function createServer(dbPath?: string): http.Server {
  const engine = new Engine(openDb(dbPath));
  return http.createServer(async (req, res) => {
    try {
      const url = new URL(req.url ?? "/", "http://localhost");
      const parts = url.pathname.split("/").filter(Boolean);
      const method = req.method ?? "GET";
      const body = method === "POST" ? await readBody(req) : {};

      if (method === "POST" && parts.length === 1 && parts[0] === "runs") {
        return send(res, 201, engine.createRun(body.name as string | undefined));
      }
      if (method === "GET" && parts.length === 1 && parts[0] === "runs") {
        return send(res, 200, engine.listRuns());
      }
      if (parts[0] === "runs" && parts.length >= 2) {
        const runId = Number(parts[1]);
        if (!Number.isSafeInteger(runId)) {
          throw new DrrError("INVALID_ARGUMENT", "runId 必须是整数");
        }
        const sub = parts[2];
        if (method === "POST" && sub === "streams") {
          engine.registerStream(runId, body.streamId as string, body.quantum as number);
          return send(res, 201, { ok: true });
        }
        if (method === "POST" && sub === "enqueue") {
          engine.enqueue(
            runId,
            body.streamId as string,
            body.packetId as string,
            body.length as number
          );
          return send(res, 201, { ok: true });
        }
        if (method === "POST" && sub === "advance") {
          const count = body.count === undefined ? 1 : (body.count as number);
          return send(res, 200, engine.advance(runId, count));
        }
        if (method === "GET" && sub === "ring") {
          return send(res, 200, engine.ring(runId));
        }
        if (method === "GET" && sub === "events") {
          const streamId = url.searchParams.get("streamId") ?? undefined;
          return send(res, 200, engine.events(runId, streamId));
        }
        if (method === "GET" && sub === "explain") {
          return send(res, 200, engine.explain(runId, Number(parts[3])));
        }
        if (method === "GET" && sub === "packets") {
          return send(res, 200, engine.packetStatus(runId, parts[3], parts[4]));
        }
        if (method === "GET" && sub === "export") {
          return send(res, 200, engine.exportRun(runId));
        }
      }
      throw new DrrError("NOT_FOUND", `未知路由 ${method} ${url.pathname}`, 404);
    } catch (err) {
      if (err instanceof DrrError) {
        return send(res, err.httpStatus, { error: err.code, message: err.message });
      }
      send(res, 500, { error: "INTERNAL", message: String(err) });
    }
  });
}

if (process.argv[1] && process.argv[1].endsWith("server.js")) {
  const portArg = process.env.PORT ? Number(process.env.PORT) : 0; // 默认选择空闲端口
  const server = createServer();
  server.listen(portArg, () => {
    const addr = server.address();
    const port = typeof addr === "object" && addr ? addr.port : portArg;
    console.log(`DRR API 服务已启动: http://127.0.0.1:${port}`);
  });
}
