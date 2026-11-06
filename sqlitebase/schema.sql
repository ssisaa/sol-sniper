-- Created automatically in sqlitebase/sniper.db. This file is the same shape.
-- No private key is stored.

CREATE TABLE IF NOT EXISTS state (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ledger (
  id TEXT PRIMARY KEY,
  ts REAL NOT NULL,
  row TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS wallet (
  mint TEXT PRIMARY KEY,
  ui_amount REAL,
  raw INTEGER,
  value_sol REAL,
  dust INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS position (
  id TEXT PRIMARY KEY,
  row TEXT NOT NULL
);
