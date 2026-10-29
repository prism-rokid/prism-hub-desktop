package db

import (
	"database/sql"
	"fmt"
)

type migration struct {
	id  string
	ddl []string
}

var migrations = []migration{
	{
		id: "0001_pluginbridge_core_v3",
		ddl: []string{
			`CREATE TABLE IF NOT EXISTS schema_migrations (
				id TEXT PRIMARY KEY,
				applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
			);`,
			`CREATE TABLE IF NOT EXISTS bridge_plugins (
				plugin_id TEXT PRIMARY KEY,
				enabled INTEGER NOT NULL DEFAULT 0,
				status TEXT NOT NULL DEFAULT 'unknown',
				surface TEXT NOT NULL DEFAULT '',
				endpoint TEXT NOT NULL DEFAULT '',
				capability_json TEXT NOT NULL DEFAULT '{}',
				visibility_status TEXT NOT NULL DEFAULT 'unknown',
				last_probe_at TEXT NOT NULL DEFAULT '',
				last_visibility_check_at TEXT NOT NULL DEFAULT '',
				last_error TEXT NOT NULL DEFAULT '',
				updated_at TEXT NOT NULL
			);`,
			`CREATE TABLE IF NOT EXISTS bridge_sessions (
				id TEXT PRIMARY KEY,
				prism_conversation_id TEXT NOT NULL,
				plugin_id TEXT NOT NULL,
				cwd TEXT NOT NULL DEFAULT '',
				native_session_id TEXT NOT NULL DEFAULT '',
				native_thread_id TEXT NOT NULL DEFAULT '',
				surface TEXT NOT NULL DEFAULT '',
				endpoint TEXT NOT NULL DEFAULT '',
				visibility_verified INTEGER NOT NULL DEFAULT 0,
				visibility_evidence TEXT NOT NULL DEFAULT '',
				created_at TEXT NOT NULL,
				updated_at TEXT NOT NULL,
				last_error TEXT NOT NULL DEFAULT '',
				UNIQUE(prism_conversation_id, plugin_id)
			);`,
			`CREATE TABLE IF NOT EXISTS bridge_events (
				id TEXT PRIMARY KEY,
				session_id TEXT NOT NULL,
				prism_conversation_id TEXT NOT NULL,
				plugin_id TEXT NOT NULL,
				native_event_id TEXT NOT NULL DEFAULT '',
				event_type TEXT NOT NULL,
				status TEXT NOT NULL DEFAULT '',
				summary TEXT NOT NULL DEFAULT '',
				payload_json TEXT NOT NULL DEFAULT '{}',
				created_at TEXT NOT NULL
			);`,
			`CREATE TABLE IF NOT EXISTS bridge_visibility_checks (
				id TEXT PRIMARY KEY,
				plugin_id TEXT NOT NULL,
				session_id TEXT NOT NULL DEFAULT '',
				marker TEXT NOT NULL,
				visible INTEGER NOT NULL DEFAULT 0,
				evidence TEXT NOT NULL DEFAULT '',
				failure_reason TEXT NOT NULL DEFAULT '',
				checked_at TEXT NOT NULL
			);`,
			`CREATE INDEX IF NOT EXISTS bridge_sessions_plugin_idx ON bridge_sessions(plugin_id);`,
			`CREATE INDEX IF NOT EXISTS bridge_sessions_native_idx ON bridge_sessions(plugin_id, native_session_id);`,
			`CREATE INDEX IF NOT EXISTS bridge_events_session_idx ON bridge_events(session_id, created_at);`,
			`CREATE INDEX IF NOT EXISTS bridge_events_conversation_idx ON bridge_events(prism_conversation_id, created_at);`,
			`CREATE INDEX IF NOT EXISTS bridge_visibility_plugin_idx ON bridge_visibility_checks(plugin_id, checked_at);`,
		},
	},
	{
		id: "0002_pluginbridge_message_receipts_v3",
		ddl: []string{
			`CREATE TABLE IF NOT EXISTS bridge_message_receipts (
				prism_message_id TEXT PRIMARY KEY,
				prism_conversation_id TEXT NOT NULL,
				plugin_id TEXT NOT NULL,
				session_id TEXT NOT NULL DEFAULT '',
				native_message_id TEXT NOT NULL DEFAULT '',
				status TEXT NOT NULL DEFAULT '',
				visible INTEGER NOT NULL DEFAULT 0,
				created_at TEXT NOT NULL,
				updated_at TEXT NOT NULL,
				last_error TEXT NOT NULL DEFAULT ''
			);`,
			`CREATE INDEX IF NOT EXISTS bridge_message_receipts_conversation_idx ON bridge_message_receipts(prism_conversation_id, created_at);`,
			`CREATE INDEX IF NOT EXISTS bridge_message_receipts_plugin_idx ON bridge_message_receipts(plugin_id, created_at);`,
		},
	},
	{
		id: "0003_pluginbridge_drop_obsolete_companion_tables_v3",
		ddl: []string{
			`DROP TABLE IF EXISTS bridge_messages;`,
			`DROP TABLE IF EXISTS bridge_conversations;`,
		},
	},
	{
		id: "0004_pluginbridge_conversation_start_v3",
		ddl: []string{
			`ALTER TABLE bridge_sessions ADD COLUMN native_identity TEXT NOT NULL DEFAULT '';`,
			`UPDATE bridge_sessions
				SET native_identity = CASE
					WHEN native_thread_id <> '' THEN 'thread:' || native_thread_id
					WHEN native_session_id <> '' THEN 'session:' || native_session_id
					ELSE ''
				END
				WHERE native_identity = '';`,
			`CREATE UNIQUE INDEX IF NOT EXISTS bridge_sessions_plugin_native_identity_unique
				ON bridge_sessions(plugin_id, native_identity)
				WHERE native_identity <> '';`,
			`CREATE TABLE IF NOT EXISTS bridge_start_operations (
				plugin_id TEXT NOT NULL,
				prism_message_id TEXT NOT NULL,
				request_id TEXT NOT NULL DEFAULT '',
				status TEXT NOT NULL,
				prism_conversation_id TEXT NOT NULL DEFAULT '',
				native_message_id TEXT NOT NULL DEFAULT '',
				last_error TEXT NOT NULL DEFAULT '',
				created_at TEXT NOT NULL,
				updated_at TEXT NOT NULL,
				PRIMARY KEY(plugin_id, prism_message_id)
			);`,
			`CREATE INDEX IF NOT EXISTS bridge_start_operations_conversation_idx
				ON bridge_start_operations(plugin_id, prism_conversation_id);`,
		},
	},
	{
		id: "0005_pluginbridge_message_receipts_plugin_key_v4",
		ddl: []string{
			`CREATE TABLE bridge_message_receipts_v4 (
				prism_message_id TEXT NOT NULL,
				prism_conversation_id TEXT NOT NULL,
				plugin_id TEXT NOT NULL,
				session_id TEXT NOT NULL DEFAULT '',
				native_message_id TEXT NOT NULL DEFAULT '',
				status TEXT NOT NULL DEFAULT '',
				visible INTEGER NOT NULL DEFAULT 0,
				created_at TEXT NOT NULL,
				updated_at TEXT NOT NULL,
				last_error TEXT NOT NULL DEFAULT '',
				PRIMARY KEY(plugin_id, prism_message_id)
			);`,
			`INSERT INTO bridge_message_receipts_v4 (
				prism_message_id, prism_conversation_id, plugin_id, session_id, native_message_id,
				status, visible, created_at, updated_at, last_error
			) SELECT
				prism_message_id, prism_conversation_id, plugin_id, session_id, native_message_id,
				status, visible, created_at, updated_at, last_error
			FROM bridge_message_receipts;`,
			`DROP TABLE bridge_message_receipts;`,
			`ALTER TABLE bridge_message_receipts_v4 RENAME TO bridge_message_receipts;`,
			`CREATE INDEX bridge_message_receipts_conversation_idx ON bridge_message_receipts(prism_conversation_id, created_at);`,
			`CREATE INDEX bridge_message_receipts_plugin_idx ON bridge_message_receipts(plugin_id, created_at);`,
		},
	},
	{
		id: "0006_pluginbridge_event_retention_indexes",
		ddl: []string{
			// 部分 v3 之前的安装会记录核心迁移但缺少事件表；建索引前补齐表，
			// 确保这类历史数据库可以继续升级。
			`CREATE TABLE IF NOT EXISTS bridge_events (
				id TEXT PRIMARY KEY,
				session_id TEXT NOT NULL,
				prism_conversation_id TEXT NOT NULL,
				plugin_id TEXT NOT NULL,
				native_event_id TEXT NOT NULL DEFAULT '',
				event_type TEXT NOT NULL,
				status TEXT NOT NULL DEFAULT '',
				summary TEXT NOT NULL DEFAULT '',
				payload_json TEXT NOT NULL DEFAULT '{}',
				created_at TEXT NOT NULL
			);`,
			`CREATE INDEX IF NOT EXISTS bridge_events_recent_idx ON bridge_events(created_at DESC);`,
			`CREATE INDEX IF NOT EXISTS bridge_events_conversation_plugin_idx ON bridge_events(prism_conversation_id, plugin_id, created_at);`,
		},
	},
	{
		id: "0007_pluginbridge_native_event_delivery",
		ddl: []string{
			`CREATE TABLE IF NOT EXISTS bridge_native_event_delivery (
				session_id TEXT NOT NULL,
				native_event_id TEXT NOT NULL,
				delivered_at TEXT NOT NULL,
				PRIMARY KEY(session_id, native_event_id)
			);`,
		},
	},
}

func runMigrations(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("prismdb: begin migrations: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if _, err = tx.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations (
		id TEXT PRIMARY KEY,
		applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
	);`); err != nil {
		return fmt.Errorf("prismdb: ensure schema_migrations: %w", err)
	}
	for _, migration := range migrations {
		var exists int
		if err = tx.QueryRow(`SELECT COUNT(1) FROM schema_migrations WHERE id = ?`, migration.id).Scan(&exists); err != nil {
			return fmt.Errorf("prismdb: check migration %s: %w", migration.id, err)
		}
		if exists > 0 {
			continue
		}
		for _, statement := range migration.ddl {
			if _, err = tx.Exec(statement); err != nil {
				return fmt.Errorf("prismdb: apply %s: %w", migration.id, err)
			}
		}
		if _, err = tx.Exec(`INSERT INTO schema_migrations (id) VALUES (?)`, migration.id); err != nil {
			return fmt.Errorf("prismdb: record migration %s: %w", migration.id, err)
		}
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("prismdb: commit migrations: %w", err)
	}
	return nil
}
