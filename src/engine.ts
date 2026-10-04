import { DatabaseSync } from "node:sqlite";
import {
  DrrError,
  LIMITS,
  assertId,
  assertPositiveInt,
  assertSafeInt,
} from "./errors.js";

export interface SentPacket {
  packetId: string;
  length: number;
}

export interface VisitEvent {
  runId: number;
  visit: number;
  streamId: string;
  deficitBefore: number;
  quantumAdded: number;
  sentPackets: SentPacket[];
  sentBytes: number;
  deficitAfter: number;
  headBlocked: string | null;
  nextStream: string | null;
  streamEmptied: boolean;
}

interface RunRow {
  id: number;
  name: string;
  visits: number;
  enqueued_packets: number;
  cursor_stream: string | null;
  started: number;
}

interface StreamRow {
  stream_id: string;
  quantum: number;
  position: number;
  active: number;
  deficit: number;
  served_bytes: number;
  backlog_bytes: number;
  backlog_packets: number;
}

export class Engine {
  constructor(private db: DatabaseSync) {}

  close(): void {
    this.db.close();
  }

  private getRun(runId: number): RunRow {
    assertSafeInt(runId, "runId");
    const row = this.db
      .prepare("SELECT * FROM runs WHERE id = ?")
      .get(runId) as RunRow | undefined;
    if (!row) throw new DrrError("UNKNOWN_RUN", `运行 ${runId} 不存在`, 404);
    return row;
  }

  private getStream(runId: number, streamId: string): StreamRow {
    const row = this.db
      .prepare("SELECT * FROM streams WHERE run_id = ? AND stream_id = ?")
      .get(runId, streamId) as StreamRow | undefined;
    if (!row)
      throw new DrrError("UNKNOWN_STREAM", `流 ${streamId} 在运行 ${runId} 中不存在`, 404);
    return row;
  }

  createRun(name?: string): { runId: number; name: string } {
    const runName = name ?? `run-${Date.now()}`;
    assertId(runName, "name");
    const info = this.db
      .prepare("INSERT INTO runs (name) VALUES (?)")
      .run(runName);
    return { runId: Number(info.lastInsertRowid), name: runName };
  }

  listRuns(): unknown[] {
    return this.db.prepare("SELECT * FROM runs ORDER BY id").all();
  }

  registerStream(runId: number, streamId: string, quantum: number): void {
    assertId(streamId, "streamId");
    assertPositiveInt(quantum, "quantum", LIMITS.MAX_QUANTUM);
    const run = this.getRun(runId);
    if (run.started) {
      throw new DrrError(
        "STREAMS_FROZEN",
        "运行已开始调度，流参数已冻结；请创建新运行"
      );
    }
    const existing = this.db
      .prepare("SELECT 1 FROM streams WHERE run_id = ? AND stream_id = ?")
      .get(runId, streamId);
    if (existing) {
      throw new DrrError("DUPLICATE_STREAM", `流 ${streamId} 已存在`);
    }
    const maxPos = this.db
      .prepare("SELECT COALESCE(MAX(position), 0) AS m FROM streams WHERE run_id = ?")
      .get(runId) as { m: number };
    this.db
      .prepare(
        "INSERT INTO streams (run_id, stream_id, quantum, position) VALUES (?, ?, ?, ?)"
      )
      .run(runId, streamId, quantum, maxPos.m + 1);
  }

  enqueue(runId: number, streamId: string, packetId: string, length: number): void {
    assertId(packetId, "packetId");
    assertPositiveInt(length, "length", LIMITS.MAX_PACKET_LENGTH);
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const run = this.getRun(runId);
      if (run.enqueued_packets >= LIMITS.MAX_PACKETS_PER_RUN) {
        throw new DrrError(
          "LIMIT_EXCEEDED",
          `运行内包数量超过上限 ${LIMITS.MAX_PACKETS_PER_RUN}`
        );
      }
      const stream = this.getStream(runId, streamId);
      const dup = this.db
        .prepare(
          "SELECT 1 FROM packets WHERE run_id = ? AND stream_id = ? AND packet_id = ?"
        )
        .get(runId, streamId, packetId);
      if (dup) {
        throw new DrrError("DUPLICATE_PACKET", `包 ${packetId} 已存在于流 ${streamId}`);
      }
      const maxSeq = this.db
        .prepare(
          "SELECT COALESCE(MAX(seq), 0) AS m FROM packets WHERE run_id = ? AND stream_id = ?"
        )
        .get(runId, streamId) as { m: number };
      this.db
        .prepare(
          "INSERT INTO packets (run_id, stream_id, packet_id, length, seq) VALUES (?, ?, ?, ?, ?)"
        )
        .run(runId, streamId, packetId, length, maxSeq.m + 1);
      this.db
        .prepare(
          "UPDATE streams SET backlog_bytes = backlog_bytes + ?, backlog_packets = backlog_packets + 1 WHERE run_id = ? AND stream_id = ?"
        )
        .run(length, runId, streamId);
      if (!stream.active) {
        // 空流重新激活：挂到环尾，不继承旧额度（变空时已清零）。
        const maxPos = this.db
          .prepare(
            "SELECT COALESCE(MAX(position), 0) AS m FROM streams WHERE run_id = ?"
          )
        .get(runId) as { m: number };
        this.db
          .prepare(
            "UPDATE streams SET active = 1, position = ?, deficit = 0 WHERE run_id = ? AND stream_id = ?"
          )
          .run(maxPos.m + 1, runId, streamId);
        if (run.cursor_stream === null) {
          this.db
            .prepare("UPDATE runs SET cursor_stream = ? WHERE id = ?")
            .run(streamId, runId);
        }
      }
      this.db
        .prepare("UPDATE runs SET enqueued_packets = enqueued_packets + 1 WHERE id = ?")
        .run(runId);
      this.db.exec("COMMIT");
    } catch (err) {
      this.db.exec("ROLLBACK");
      throw err;
    }
  }

  private nextActive(runId: number, afterPosition: number): string | null {
    const row = this.db
      .prepare(
        `SELECT stream_id FROM streams
         WHERE run_id = ? AND active = 1
         ORDER BY (position <= ?) ASC, position ASC
         LIMIT 1`
      )
      .get(runId, afterPosition) as { stream_id: string } | undefined;
    return row ? row.stream_id : null;
  }

  advance(runId: number, count = 1): VisitEvent[] {
    assertPositiveInt(count, "count", LIMITS.MAX_VISITS_PER_ADVANCE);
    const events: VisitEvent[] = [];
    for (let i = 0; i < count; i++) {
      events.push(this.visitOnce(runId));
    }
    return events;
  }

  private visitOnce(runId: number): VisitEvent {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const run = this.getRun(runId);
      if (run.cursor_stream === null) {
        throw new DrrError("NO_ACTIVE_STREAMS", "活动环为空，没有可服务的流", 409);
      }
      const streamId = run.cursor_stream;
      const stream = this.getStream(runId, streamId);
      const deficitBefore = stream.deficit;
      let deficit = deficitBefore + stream.quantum;
      const sentPackets: SentPacket[] = [];
      let sentBytes = 0;

      const headStmt = this.db.prepare(
        `SELECT packet_id, length FROM packets
         WHERE run_id = ? AND stream_id = ? AND status = 'queued'
         ORDER BY seq ASC LIMIT 1`
      );
      let headBlocked: string | null = null;
      for (;;) {
        const head = headStmt.get(runId, streamId) as
          | { packet_id: string; length: number }
          | undefined;
        if (!head) break;
        if (head.length > deficit) {
          headBlocked = head.packet_id;
          break;
        }
        deficit -= head.length;
        sentBytes += head.length;
        sentPackets.push({ packetId: head.packet_id, length: head.length });
        this.db
          .prepare(
            "UPDATE packets SET status = 'sent', sent_visit = ? WHERE run_id = ? AND stream_id = ? AND packet_id = ?"
          )
          .run(run.visits + 1, runId, streamId, head.packet_id);
      }

      const remaining = this.db
        .prepare(
          "SELECT COUNT(*) AS c, COALESCE(SUM(length),0) AS b FROM packets WHERE run_id = ? AND stream_id = ? AND status = 'queued'"
        )
        .get(runId, streamId) as { c: number; b: number };

      const emptied = remaining.c === 0;
      let nextStream: string | null;
      if (emptied) {
        // 队列变空：额度归零并移出活动环。
        deficit = 0;
        this.db
          .prepare(
            "UPDATE streams SET active = 0, deficit = 0, served_bytes = served_bytes + ?, backlog_bytes = 0, backlog_packets = 0 WHERE run_id = ? AND stream_id = ?"
          )
          .run(sentBytes, runId, streamId);
        nextStream = this.nextActive(runId, stream.position);
      } else {
        this.db
          .prepare(
            "UPDATE streams SET deficit = ?, served_bytes = served_bytes + ?, backlog_bytes = ?, backlog_packets = ? WHERE run_id = ? AND stream_id = ?"
          )
          .run(deficit, sentBytes, remaining.b, remaining.c, runId, streamId);
        nextStream = this.nextActive(runId, stream.position);
      }

      const visit = run.visits + 1;
      this.db
        .prepare(
          `INSERT INTO events (run_id, visit, stream_id, deficit_before, quantum_added, deficit_after, sent_packets, sent_bytes, head_blocked, next_stream)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
        )
        .run(
          runId,
          visit,
          streamId,
          deficitBefore,
          stream.quantum,
          deficit,
          JSON.stringify(sentPackets),
          sentBytes,
          headBlocked,
          nextStream
        );
      this.db
        .prepare(
          "UPDATE runs SET visits = ?, cursor_stream = ?, started = 1 WHERE id = ?"
        )
        .run(visit, nextStream, runId);
      this.db.exec("COMMIT");
      return {
        runId,
        visit,
        streamId,
        deficitBefore,
        quantumAdded: stream.quantum,
        sentPackets,
        sentBytes,
        deficitAfter: deficit,
        headBlocked,
        nextStream,
        streamEmptied: emptied,
      };
    } catch (err) {
      this.db.exec("ROLLBACK");
      throw err;
    }
  }

  ring(runId: number): { cursor: string | null; streams: unknown[] } {
    const run = this.getRun(runId);
    const rows = this.db
      .prepare(
        `SELECT stream_id AS streamId, quantum, deficit, active, served_bytes AS servedBytes,
                backlog_bytes AS backlogBytes, backlog_packets AS backlogPackets, position
         FROM streams WHERE run_id = ? ORDER BY position`
      )
      .all(runId);
    return { cursor: run.cursor_stream, streams: rows };
  }

  events(runId: number, streamId?: string): unknown[] {
    this.getRun(runId);
    const rows = (
      streamId
        ? this.db
            .prepare("SELECT * FROM events WHERE run_id = ? AND stream_id = ? ORDER BY visit")
            .all(runId, streamId)
        : this.db
            .prepare("SELECT * FROM events WHERE run_id = ? ORDER BY visit")
            .all(runId)
    ) as Array<Record<string, unknown>>;
    return rows.map((r) => ({ ...r, sent_packets: JSON.parse(r.sent_packets as string) }));
  }

  explain(runId: number, visit: number): unknown {
    assertSafeInt(visit, "visit");
    this.getRun(runId);
    const row = this.db
      .prepare("SELECT * FROM events WHERE run_id = ? AND visit = ?")
      .get(runId, visit) as Record<string, unknown> | undefined;
    if (!row) throw new DrrError("NOT_FOUND", `访问记录 ${visit} 不存在`, 404);
    const sent = JSON.parse(row.sent_packets as string) as SentPacket[];
    const sentDesc =
      sent.length > 0
        ? sent.map((p) => `${p.packetId}(${p.length}B)`).join(", ")
        : "无";
    const explanation =
      `第 ${row.visit} 次访问流 ${row.stream_id}：` +
      `额度 ${row.deficit_before} + quantum ${row.quantum_added} = ` +
      `${(row.deficit_before as number) + (row.quantum_added as number)}；` +
      `发送 ${sentDesc}，共 ${row.sent_bytes} 字节；` +
      `访问后额度 ${row.deficit_after}` +
      (row.head_blocked ? `；队首 ${row.head_blocked} 长度超过剩余额度，保留结转` : "") +
      `；下一流 ${row.next_stream ?? "(环空)"}。`;
    return { ...row, sent_packets: sent, explanation };
  }

  packetStatus(runId: number, streamId: string, packetId: string): unknown {
    this.getRun(runId);
    const row = this.db
      .prepare(
        "SELECT * FROM packets WHERE run_id = ? AND stream_id = ? AND packet_id = ?"
      )
      .get(runId, streamId, packetId);
    if (!row)
      throw new DrrError("UNKNOWN_PACKET", `包 ${packetId} 不存在`, 404);
    return row;
  }

  exportRun(runId: number): unknown {
    const run = this.getRun(runId);
    return {
      run,
      ring: this.ring(runId),
      events: this.events(runId),
      packets: this.db
        .prepare("SELECT * FROM packets WHERE run_id = ? ORDER BY stream_id, seq")
        .all(runId),
    };
  }
}
