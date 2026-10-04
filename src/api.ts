import * as http from "node:http";
import { AddressInfo } from "node:net";
import { DrrEngine } from "./engine";
import { DrrError } from "./config";

function sendJson(res: http.ServerResponse, status: number, body: unknown): void {
  const data = JSON.stringify(body, null, 2);
  res.writeHead(status, { "Content-Type": "application/json; charset=utf-8" });
  res.end(data);
}

async function readBody(req: http.IncomingMessage): Promise<any> {
  const chunks: Buffer[] = [];
  for await (const c of req) chunks.push(c as Buffer);
  if (chunks.length === 0) return {};
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    throw new DrrError("INVALID_ARGUMENT", "请求体必须是合法 JSON");
  }
}

export async function startServer(engine: DrrEngine, port = 0): Promise<{ server: http.Server; port: number }> {
  const server = http.createServer(async (req, res) => {
    try {
      const url = new URL(req.url ?? "/", "http://localhost");
      const parts = url.pathname.split("/").filter(Boolean);
      const method = req.method ?? "GET";
      const body = method === "POST" ? await readBody(req) : {};

      // POST /runs {name?}
      if (method === "POST" && parts.length === 1 && parts[0] === "runs") {
        return sendJson(res, 201, engine.createRun(body.name));
      }
      if (method === "GET" && parts.length === 1 && parts[0] === "runs") {
        return sendJson(res, 200, engine.listRuns());
      }
      if (parts[0] === "runs" && parts.length >= 2) {
        const runId = Number(parts[1]);
        const sub = parts[2];
        // POST /runs/:id/streams {streamId, quantum}
        if (method === "POST" && sub === "streams" && parts.length === 3) {
          return sendJson(res, 201, engine.addStream(runId, body.streamId, body.quantum));
        }
        // POST /runs/:id/enqueue {streamId, packetId, length}
        if (method === "POST" && sub === "enqueue" && parts.length === 3) {
          return sendJson(res, 201, engine.enqueue(runId, body.streamId, body.packetId, body.length));
        }
        // POST /runs/:id/advance {count?}
        if (method === "POST" && sub === "advance" && parts.length === 3) {
          const events = engine.advance(runId, body.count ?? 1);
          if (events.length === 0) {
            return sendJson(res, 200, { message: "活动环为空，无可服务流", events });
          }
          return sendJson(res, 200, { events });
        }
        if (method === "GET" && sub === "ring") return sendJson(res, 200, engine.getRing(runId));
        if (method === "GET" && sub === "events" && parts.length === 3) {
          return sendJson(res, 200, engine.listEvents(runId, url.searchParams.get("streamId") ?? undefined));
        }
        // GET /runs/:id/explain/:seq
        if (method === "GET" && sub === "explain" && parts.length === 4) {
          return sendJson(res, 200, { explanation: engine.explainEvent(runId, Number(parts[3])) });
        }
        // GET /runs/:id/packets/:packetId
        if (method === "GET" && sub === "packets" && parts.length === 4) {
          return sendJson(res, 200, engine.getPacket(runId, decodeURIComponent(parts[3])));
        }
        if (method === "GET" && sub === "history") return sendJson(res, 200, engine.history(runId));
        if (method === "GET" && sub === "export") return sendJson(res, 200, engine.exportRun(runId));
      }
      throw new DrrError("INVALID_ARGUMENT", `未知路由: ${method} ${url.pathname}`, 404);
    } catch (e) {
      if (e instanceof DrrError) {
        return sendJson(res, e.status, { error: { code: e.code, message: e.message } });
      }
      return sendJson(res, 500, { error: { code: "INTERNAL", message: String(e) } });
    }
  });
  await new Promise<void>((resolve) => server.listen(port, resolve));
  const actual = (server.address() as AddressInfo).port;
  return { server, port: actual };
}
