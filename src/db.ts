import { DatabaseSync } from "node:sqlite";
import fs from "node:fs";
import path from "node:path";

export function defaultDataDir(): string {
  return process.env.DRR_DATA_DIR ?? path.join(process.cwd(), "data");
}

export function openDb(dbPath?: string): DatabaseSync {
  const file = dbPath ?? path.join(defaultDataDir(), "drr.db");
  if (file !== ":memory:") {
    fs.mkdirSync(path.dirname(file), { recursive: true });
  }
  const db = new DatabaseSync(file);
  db.exec("PRAGMA journal_mode = WAL;");
  db.exec("PRAGMA foreign_keys = ON;");
  migrate(db);
  return db;
}

function migrate(db: DatabaseSync): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS runs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
      visits INTEGER NOT NULL DEFAULT 0,
      enqueued_packets INTEGER NOT NULL DEFAULT 0,
      cursor_stream TEXT,
      started INTEGER NOT NULL DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS streams (
      run_id INTEGER NOT NULL REFERENCES runs(id),
      stream_id TEXT NOT NULL,
      quantum INTEGER NOT NULL,
      position INTEGER NOT NULL,
      active INTEGER NOT NULL DEFAULT 0,
      deficit INTEGER NOT NULL DEFAULT 0,
      served_bytes INTEGER NOT NULL DEFAULT 0,
      backlog_bytes INTEGER NOT NULL DEFAULT 0,
      backlog_packets INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (run_id, stream_id)
    );
    CREATE TABLE IF NOT EXISTS packets (
      run_id INTEGER NOT NULL,
      stream_id TEXT NOT NULL,
      packet_id TEXT NOT NULL,
      length INTEGER NOT NULL,
      seq INTEGER NOT NULL,
      status TEXT NOT NULL DEFAULT 'queued',
      sent_visit INTEGER,
      PRIMARY KEY (run_id, stream_id, packet_id),
      FOREIGN KEY (run_id, stream_id) REFERENCES streams(run_id, stream_id)
    );
    CREATE TABLE IF NOT EXISTS events (
      run_id INTEGER NOT NULL REFERENCES runs(id),
      visit INTEGER NOT NULL,
      stream_id TEXT NOT NULL,
      deficit_before INTEGER NOT NULL,
      quantum_added INTEGER NOT NULL,
      deficit_after INTEGER NOT NULL,
      sent_packets TEXT NOT NULL,
      sent_bytes INTEGER NOT NULL,
      head_blocked TEXT,
      next_stream TEXT,
      PRIMARY KEY (run_id, visit)
    );
  `);
}
