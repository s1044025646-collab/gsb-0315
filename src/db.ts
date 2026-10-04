import { DatabaseSync } from "node:sqlite";
import * as fs from "node:fs";
import * as path from "node:path";

export const DATA_DIR = process.env.DRR_DATA_DIR ?? path.join(process.cwd(), "data");
export const DB_PATH = process.env.DRR_DB_PATH ?? path.join(DATA_DIR, "drr.db");

export function openDb(dbPath: string = DB_PATH): DatabaseSync {
  if (dbPath !== ":memory:") {
    fs.mkdirSync(path.dirname(dbPath), { recursive: true });
  }
  const db = new DatabaseSync(dbPath);
  db.exec("PRAGMA journal_mode = WAL;");
  db.exec("PRAGMA foreign_keys = ON;");
  migrate(db);
  return db;
}

function migrate(db: DatabaseSync): void {
  db.exec(`
  CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    started INTEGER NOT NULL DEFAULT 0,          -- 首次推进后冻结流参数
    cursor_stream_id INTEGER,                    -- 下一次要访问的活动流
    next_event_seq INTEGER NOT NULL DEFAULT 0,
    next_activation_seq INTEGER NOT NULL DEFAULT 0
  );
  CREATE TABLE IF NOT EXISTS streams (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES runs(id),
    stream_key TEXT NOT NULL,
    quantum INTEGER NOT NULL,
    active INTEGER NOT NULL DEFAULT 0,
    activation_seq INTEGER NOT NULL DEFAULT -1,  -- 活动环顺序 = 激活顺序
    deficit INTEGER NOT NULL DEFAULT 0,
    served_bytes INTEGER NOT NULL DEFAULT 0,
    backlog_bytes INTEGER NOT NULL DEFAULT 0,
    enqueued_bytes INTEGER NOT NULL DEFAULT 0,
    enqueued_packets INTEGER NOT NULL DEFAULT 0,
    UNIQUE (run_id, stream_key)
  );
  CREATE TABLE IF NOT EXISTS packets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES runs(id),
    stream_id INTEGER NOT NULL REFERENCES streams(id),
    packet_key TEXT NOT NULL,
    length INTEGER NOT NULL,
    queue_seq INTEGER NOT NULL,                  -- 流内 FIFO 顺序
    status TEXT NOT NULL DEFAULT 'queued',       -- queued | sent
    sent_event_seq INTEGER,
    UNIQUE (run_id, packet_key)
  );
  CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES runs(id),
    seq INTEGER NOT NULL,
    stream_id INTEGER NOT NULL,
    stream_key TEXT NOT NULL,
    quantum INTEGER NOT NULL,
    deficit_before INTEGER NOT NULL,
    added INTEGER NOT NULL,
    deficit_after INTEGER NOT NULL,
    sent_packets TEXT NOT NULL,                  -- JSON: [{packetKey,length}]
    sent_bytes INTEGER NOT NULL,
    head_blocked TEXT,                           -- JSON: {packetKey,length} 或 NULL
    queue_emptied INTEGER NOT NULL,
    next_stream_key TEXT,
    UNIQUE (run_id, seq)
  );
  CREATE INDEX IF NOT EXISTS idx_packets_stream ON packets(stream_id, status, queue_seq);
  CREATE INDEX IF NOT EXISTS idx_events_run ON events(run_id, seq);
  `);
}
