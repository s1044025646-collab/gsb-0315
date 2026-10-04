import { DatabaseSync } from "node:sqlite";
import { DrrError, LIMITS, assertInt, assertKey } from "./config";

export interface StreamRow {
  id: number; run_id: number; stream_key: string; quantum: number;
  active: number; activation_seq: number; deficit: number;
  served_bytes: number; backlog_bytes: number;
  enqueued_bytes: number; enqueued_packets: number;
}
export interface RunRow {
  id: number; name: string; started: number; cursor_stream_id: number | null;
  next_event_seq: number; next_activation_seq: number;
}
export interface PacketRow {
  id: number; run_id: number; stream_id: number; packet_key: string;
  length: number; queue_seq: number; status: string; sent_event_seq: number | null;
}

export interface SentItem { packetKey: string; length: number; }
export interface VisitEvent {
  seq: number; streamKey: string; quantum: number;
  deficitBefore: number; added: number; deficitAfter: number;
  sentPackets: SentItem[]; sentBytes: number;
  headBlocked: { packetKey: string; length: number } | null;
  queueEmptied: boolean; nextStreamKey: string | null;
}

export class DrrEngine {
  constructor(private db: DatabaseSync) {}

  createRun(name: unknown = "") {
    if (name !== undefined && name !== null && typeof name !== "string") {
      throw new DrrError("INVALID_ARGUMENT", "name 必须是字符串");
    }
    const info = this.db.prepare("INSERT INTO runs (name) VALUES (?)").run((name as string) ?? "");
    return this.getRun(Number(info.lastInsertRowid));
  }

  getRun(runId: unknown): RunRow {
    const id = assertInt(runId, "runId", 1, Number.MAX_SAFE_INTEGER);
    const run = this.db.prepare("SELECT * FROM runs WHERE id = ?").get(id) as RunRow | undefined;
    if (!run) throw new DrrError("RUN_NOT_FOUND", `运行 ${id} 不存在`, 404);
    return run;
  }

  listRuns() {
    return this.db.prepare("SELECT * FROM runs ORDER BY id").all();
  }

  addStream(runId: unknown, streamKey: unknown, quantum: unknown) {
    const run = this.getRun(runId);
    const key = assertKey(streamKey, "streamId");
    const q = assertInt(quantum, "quantum", 1, LIMITS.MAX_QUANTUM);
    if (run.started) {
      throw new DrrError("RUN_SEALED", "运行已开始推进，流参数已冻结；如需不同 quantum 请创建新运行", 409);
    }
    const cnt = this.db.prepare("SELECT COUNT(*) AS c FROM streams WHERE run_id = ?").get(run.id) as { c: number };
    if (cnt.c >= LIMITS.MAX_STREAMS_PER_RUN) {
      throw new DrrError("LIMIT_EXCEEDED", `单运行流数量超过上限 ${LIMITS.MAX_STREAMS_PER_RUN}`);
    }
    const dup = this.db.prepare("SELECT 1 FROM streams WHERE run_id = ? AND stream_key = ?").get(run.id, key);
    if (dup) throw new DrrError("DUPLICATE_STREAM", `流 '${key}' 已存在于运行 ${run.id}`, 409);
    const info = this.db.prepare(
      "INSERT INTO streams (run_id, stream_key, quantum) VALUES (?, ?, ?)"
    ).run(run.id, key, q);
    return this.db.prepare("SELECT * FROM streams WHERE id = ?").get(Number(info.lastInsertRowid));
  }

  enqueue(runId: unknown, streamKey: unknown, packetKey: unknown, length: unknown) {
    const run = this.getRun(runId);
    const sKey = assertKey(streamKey, "streamId");
    const pKey = assertKey(packetKey, "packetId");
    const len = assertInt(length, "length", 1, LIMITS.MAX_PACKET_LENGTH);
    const stream = this.db.prepare(
      "SELECT * FROM streams WHERE run_id = ? AND stream_key = ?"
    ).get(run.id, sKey) as StreamRow | undefined;
    if (!stream) throw new DrrError("STREAM_NOT_FOUND", `运行 ${run.id} 中不存在流 '${sKey}'`, 404);
    const dup = this.db.prepare("SELECT 1 FROM packets WHERE run_id = ? AND packet_key = ?").get(run.id, pKey);
    if (dup) throw new DrrError("DUPLICATE_PACKET", `包编号 '${pKey}' 在运行 ${run.id} 中已存在`, 409);
    const cnt = this.db.prepare("SELECT COUNT(*) AS c FROM packets WHERE run_id = ?").get(run.id) as { c: number };
    if (cnt.c >= LIMITS.MAX_PACKETS_PER_RUN) {
      throw new DrrError("LIMIT_EXCEEDED", `单运行包数量超过上限 ${LIMITS.MAX_PACKETS_PER_RUN}`);
    }

    this.tx(() => {
      const seqRow = this.db.prepare(
        "SELECT COALESCE(MAX(queue_seq), -1) + 1 AS s FROM packets WHERE stream_id = ?"
      ).get(stream.id) as { s: number };
      this.db.prepare(
        "INSERT INTO packets (run_id, stream_id, packet_key, length, queue_seq) VALUES (?, ?, ?, ?, ?)"
      ).run(run.id, stream.id, pKey, len, seqRow.s);
      this.db.prepare(
        "UPDATE streams SET backlog_bytes = backlog_bytes + ?, enqueued_bytes = enqueued_bytes + ?, enqueued_packets = enqueued_packets + 1 WHERE id = ?"
      ).run(len, len, stream.id);
      if (!stream.active) {
        // 空流重新激活：额度归零（不继承空闲前剩余），排到环尾
        const act = run.next_activation_seq;
        this.db.prepare(
          "UPDATE streams SET active = 1, deficit = 0, activation_seq = ? WHERE id = ?"
        ).run(act, stream.id);
        this.db.prepare("UPDATE runs SET next_activation_seq = ? WHERE id = ?").run(act + 1, run.id);
        if (run.cursor_stream_id === null) {
          this.db.prepare("UPDATE runs SET cursor_stream_id = ? WHERE id = ?").run(stream.id, run.id);
        }
      }
    });
    return this.db.prepare("SELECT * FROM packets WHERE run_id = ? AND packet_key = ?").get(run.id, pKey);
  }

  /** 推进 count 次访问，整体在一个事务中；返回本次产生的访问事件。 */
  advance(runId: unknown, count: unknown = 1): VisitEvent[] {
    const run = this.getRun(runId);
    const n = assertInt(count, "count", 1, LIMITS.MAX_ADVANCE_PER_CALL);
    const events: VisitEvent[] = [];
    this.tx(() => {
      for (let i = 0; i < n; i++) {
        const ev = this.visitOnce(run.id);
        if (!ev) break; // 空环：无可服务流，不产生访问记录
        events.push(ev);
      }
    });
    return events;
  }

  private visitOnce(runId: number): VisitEvent | null {
    const run = this.db.prepare("SELECT * FROM runs WHERE id = ?").get(runId) as unknown as RunRow;
    if (run.cursor_stream_id === null) return null;
    const stream = this.db.prepare("SELECT * FROM streams WHERE id = ? AND active = 1").get(run.cursor_stream_id) as StreamRow | undefined;
    if (!stream) return null;

    const deficitBefore = stream.deficit;
    let deficit = deficitBefore + stream.quantum; // 每次访问先追加一个 quantum
    if (deficit > LIMITS.MAX_DEFICIT) {
      throw new DrrError("LIMIT_EXCEEDED", `流 '${stream.stream_key}' 额度超过安全上限`);
    }
    const sent: SentItem[] = [];
    let sentBytes = 0;

    // 从队首连续发送长度不超过当前额度的包；不能跳过大包
    while (true) {
      const head = this.db.prepare(
        "SELECT * FROM packets WHERE stream_id = ? AND status = 'queued' ORDER BY queue_seq LIMIT 1"
      ).get(stream.id) as PacketRow | undefined;
      if (!head || head.length > deficit) break;
      deficit -= head.length;
      sentBytes += head.length;
      sent.push({ packetKey: head.packet_key, length: head.length });
      this.db.prepare("UPDATE packets SET status = 'sent', sent_event_seq = ? WHERE id = ?")
        .run(run.next_event_seq, head.id);
    }

    const remaining = this.db.prepare(
      "SELECT COUNT(*) AS c FROM packets WHERE stream_id = ? AND status = 'queued'"
    ).get(stream.id) as { c: number };
    const queueEmptied = remaining.c === 0;
    if (queueEmptied) deficit = 0; // 队列变空：额度归零并移出活动环

    const headBlockedRow = this.db.prepare(
      "SELECT packet_key, length FROM packets WHERE stream_id = ? AND status = 'queued' ORDER BY queue_seq LIMIT 1"
    ).get(stream.id) as { packet_key: string; length: number } | undefined;
    const headBlocked = headBlockedRow ? { packetKey: headBlockedRow.packet_key, length: headBlockedRow.length } : null;

    // 下一活动流：按激活顺序在原环中当前节点之后（当前节点可能已删除）
    const next = this.db.prepare(
      `SELECT stream_key, id FROM streams
       WHERE run_id = ? AND active = 1 AND id != ? AND activation_seq > ?
       ORDER BY activation_seq LIMIT 1`
    ).get(runId, stream.id, stream.activation_seq) as { stream_key: string; id: number } | undefined
      ?? this.db.prepare(
        `SELECT stream_key, id FROM streams
         WHERE run_id = ? AND active = 1 AND id != ?
         ORDER BY activation_seq LIMIT 1`
      ).get(runId, stream.id) as { stream_key: string; id: number } | undefined;

    const seq = run.next_event_seq;
    const nextStreamKey = next ? next.stream_key : (queueEmptied ? null : stream.stream_key);
    const cursorId = next ? next.id : (queueEmptied ? null : stream.id);
    this.db.prepare(
      `INSERT INTO events (run_id, seq, stream_id, stream_key, quantum, deficit_before, added,
        deficit_after, sent_packets, sent_bytes, head_blocked, queue_emptied, next_stream_key)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    ).run(runId, seq, stream.id, stream.stream_key, stream.quantum, deficitBefore, stream.quantum,
      deficit, JSON.stringify(sent), sentBytes, headBlocked ? JSON.stringify(headBlocked) : null,
      queueEmptied ? 1 : 0, nextStreamKey);

    this.db.prepare(
      "UPDATE streams SET deficit = ?, active = ?, served_bytes = served_bytes + ?, backlog_bytes = backlog_bytes - ? WHERE id = ?"
    ).run(deficit, queueEmptied ? 0 : 1, sentBytes, sentBytes, stream.id);
    this.db.prepare(
      "UPDATE runs SET started = 1, cursor_stream_id = ?, next_event_seq = ? WHERE id = ?"
    ).run(cursorId, seq + 1, runId);

    return {
      seq, streamKey: stream.stream_key, quantum: stream.quantum,
      deficitBefore, added: stream.quantum, deficitAfter: deficit,
      sentPackets: sent, sentBytes, headBlocked, queueEmptied,
      nextStreamKey,
    };
  }

  getRing(runId: unknown) {
    const run = this.getRun(runId);
    const rows = this.db.prepare(
      "SELECT stream_key, quantum, deficit, activation_seq FROM streams WHERE run_id = ? AND active = 1 ORDER BY activation_seq"
    ).all(run.id) as unknown as { stream_key: string; quantum: number; deficit: number; activation_seq: number }[];
    const cursor = run.cursor_stream_id === null ? null :
      (this.db.prepare("SELECT stream_key FROM streams WHERE id = ?").get(run.cursor_stream_id) as { stream_key: string } | undefined)?.stream_key ?? null;
    return {
      runId: run.id,
      cursor,
      ring: rows.map(r => ({ streamKey: r.stream_key, quantum: r.quantum, deficit: r.deficit })),
    };
  }

  listEvents(runId: unknown, streamKey?: unknown): VisitEvent[] {
    const run = this.getRun(runId);
    let rows;
    if (streamKey !== undefined && streamKey !== null) {
      const key = assertKey(streamKey, "streamId");
      rows = this.db.prepare("SELECT * FROM events WHERE run_id = ? AND stream_key = ? ORDER BY seq").all(run.id, key);
    } else {
      rows = this.db.prepare("SELECT * FROM events WHERE run_id = ? ORDER BY seq").all(run.id);
    }
    return (rows as any[]).map(rowToEvent);
  }

  explainEvent(runId: unknown, seq: unknown): string {
    const run = this.getRun(runId);
    const s = assertInt(seq, "seq", 0, Number.MAX_SAFE_INTEGER);
    const row = this.db.prepare("SELECT * FROM events WHERE run_id = ? AND seq = ?").get(run.id, s) as any;
    if (!row) throw new DrrError("EVENT_NOT_FOUND", `运行 ${run.id} 中不存在事件 #${s}`, 404);
    const ev = rowToEvent(row);
    const sentDesc = ev.sentPackets.length === 0
      ? "未发送任何包"
      : `发送 ${ev.sentPackets.map(p => `${p.packetKey}(${p.length}B)`).join(", ")}，共 ${ev.sentBytes} 字节`;
    const blockedDesc = ev.headBlocked
      ? `队首 ${ev.headBlocked.packetKey}(${ev.headBlocked.length}B) 超过剩余额度，本次停止，额度保留到下次访问`
      : "队列为空或队首可发";
    const emptyDesc = ev.queueEmptied ? "队列已清空，额度归零并移出活动环" : "队列仍有积压，保留在活动环";
    const nextDesc = ev.nextStreamKey ? `下一访问流为 '${ev.nextStreamKey}'` : "活动环已空，无下一流";
    return [
      `事件 #${ev.seq}：访问流 '${ev.streamKey}'（quantum=${ev.quantum}）`,
      `  访问前额度 ${ev.deficitBefore}，追加 ${ev.added}，可用 ${ev.deficitBefore + ev.added}`,
      `  ${sentDesc}；${blockedDesc}`,
      `  访问后额度 ${ev.deficitAfter}；${emptyDesc}；${nextDesc}`,
    ].join("\n");
  }

  getPacket(runId: unknown, packetKey: unknown) {
    const run = this.getRun(runId);
    const key = assertKey(packetKey, "packetId");
    const row = this.db.prepare(
      `SELECT p.*, s.stream_key FROM packets p JOIN streams s ON s.id = p.stream_id
       WHERE p.run_id = ? AND p.packet_key = ?`
    ).get(run.id, key) as any;
    if (!row) throw new DrrError("PACKET_NOT_FOUND", `运行 ${run.id} 中不存在包 '${key}'`, 404);
    return {
      packetKey: row.packet_key, streamKey: row.stream_key, length: row.length,
      status: row.status, queueSeq: row.queue_seq, sentEventSeq: row.sent_event_seq,
    };
  }

  history(runId: unknown) {
    const run = this.getRun(runId);
    const rows = this.db.prepare("SELECT * FROM streams WHERE run_id = ? ORDER BY id").all(run.id) as unknown as StreamRow[];
    return {
      runId: run.id,
      streams: rows.map(s => ({
        streamKey: s.stream_key, quantum: s.quantum, active: !!s.active,
        deficit: s.deficit, servedBytes: s.served_bytes, backlogBytes: s.backlog_bytes,
        enqueuedBytes: s.enqueued_bytes, enqueuedPackets: s.enqueued_packets,
        // 守恒校验：入队字节 = 已服务 + 积压
        conserved: s.enqueued_bytes === s.served_bytes + s.backlog_bytes,
      })),
    };
  }

  exportRun(runId: unknown) {
    const run = this.getRun(runId);
    return {
      run,
      streams: this.db.prepare("SELECT * FROM streams WHERE run_id = ? ORDER BY id").all(run.id),
      packets: this.db.prepare("SELECT * FROM packets WHERE run_id = ? ORDER BY id").all(run.id),
      events: this.listEvents(run.id),
    };
  }

  private tx<T>(fn: () => T): T {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const r = fn();
      this.db.exec("COMMIT");
      return r;
    } catch (e) {
      this.db.exec("ROLLBACK");
      throw e;
    }
  }
}

function rowToEvent(row: any): VisitEvent {
  return {
    seq: row.seq, streamKey: row.stream_key, quantum: row.quantum,
    deficitBefore: row.deficit_before, added: row.added, deficitAfter: row.deficit_after,
    sentPackets: JSON.parse(row.sent_packets), sentBytes: row.sent_bytes,
    headBlocked: row.head_blocked ? JSON.parse(row.head_blocked) : null,
    queueEmptied: !!row.queue_emptied, nextStreamKey: row.next_stream_key,
  };
}
