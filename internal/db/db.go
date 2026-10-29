// Package db owns Prism local SQLite state.
package db

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"rokid-prism-hub/internal/host"
)

const sqliteDriverName = "sqlite3"

const sqliteBusyTimeout = 10 * time.Second

type DB struct {
	sql *sql.DB
}

func DefaultPath() string {
	return host.DefaultDataPath("pluginbridge.sqlite")
}

func Open(path string) (*DB, error) {
	path = filepath.Clean(path)
	if path == "" || path == "." {
		return nil, fmt.Errorf("prismdb: path required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("prismdb: mkdir %s: %w", filepath.Dir(path), err)
	}
	conn, err := sql.Open(sqliteDriverName, path)
	if err != nil {
		return nil, fmt.Errorf("prismdb: open %s: %w", path, err)
	}
	// Hub owns one local state database. One shared SQLite connection keeps all
	// in-process writes ordered; busy_timeout handles a short overlap with an
	// older Hub or an explicit maintenance command using the same file.
	conn.SetMaxOpenConns(1)
	conn.SetMaxIdleConns(1)
	db := &DB{sql: conn}
	if err := db.init(); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return db, nil
}

func (db *DB) Close() error {
	if db == nil || db.sql == nil {
		return nil
	}
	return db.sql.Close()
}

func (db *DB) SQL() *sql.DB {
	if db == nil {
		return nil
	}
	return db.sql
}

func (db *DB) init() error {
	if db == nil || db.sql == nil {
		return fmt.Errorf("prismdb: db unavailable")
	}
	if _, err := db.sql.Exec(fmt.Sprintf(`PRAGMA busy_timeout=%d;`, sqliteBusyTimeout.Milliseconds())); err != nil {
		return fmt.Errorf("prismdb: configure busy timeout: %w", err)
	}
	if _, err := db.sql.Exec(`PRAGMA journal_mode=WAL;`); err != nil {
		return fmt.Errorf("prismdb: enable wal: %w", err)
	}
	return runMigrations(db.sql)
}
