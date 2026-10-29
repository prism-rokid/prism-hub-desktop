package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type PluginRecord struct {
	PluginID              string
	Enabled               bool
	Status                string
	Surface               string
	Endpoint              string
	Capability            map[string]any
	VisibilityStatus      string
	LastProbeAt           time.Time
	LastVisibilityCheckAt time.Time
	LastError             string
	UpdatedAt             time.Time
}

type SessionRecord struct {
	ID                  string
	PrismConversationID string
	PluginID            string
	Cwd                 string
	NativeSessionID     string
	NativeThreadID      string
	Surface             string
	Endpoint            string
	VisibilityVerified  bool
	VisibilityEvidence  string
	CreatedAt           time.Time
	UpdatedAt           time.Time
	LastError           string
}

// StartOperationRecord is an internal idempotency ledger for the first message
// of a newly started native conversation. It never represents a Mobile-facing
// temporary conversation.
type StartOperationRecord struct {
	PluginID            string
	PrismMessageID      string
	RequestID           string
	Status              string
	PrismConversationID string
	NativeMessageID     string
	LastError           string
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

const DefaultEventRetentionLimit = 50_000

func (db *DB) UpsertPlugin(ctx context.Context, record PluginRecord) (PluginRecord, error) {
	if db == nil || db.sql == nil {
		return PluginRecord{}, fmt.Errorf("prismdb: db unavailable")
	}
	record.PluginID = strings.ToLower(strings.TrimSpace(record.PluginID))
	if record.PluginID == "" {
		return PluginRecord{}, fmt.Errorf("prismdb: plugin_id required")
	}
	record.UpdatedAt = time.Now().UTC()
	if strings.TrimSpace(record.Status) == "" {
		record.Status = "unknown"
	}
	capabilityJSON, err := json.Marshal(record.Capability)
	if err != nil {
		return PluginRecord{}, fmt.Errorf("prismdb: marshal plugin capability: %w", err)
	}
	_, err = db.sql.ExecContext(ctx, `INSERT INTO bridge_plugins (
		plugin_id, enabled, status, surface, endpoint, capability_json, visibility_status,
		last_probe_at, last_visibility_check_at, last_error, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(plugin_id) DO UPDATE SET
		enabled = excluded.enabled,
		status = excluded.status,
		surface = excluded.surface,
		endpoint = excluded.endpoint,
		capability_json = excluded.capability_json,
		visibility_status = excluded.visibility_status,
		last_probe_at = excluded.last_probe_at,
		last_visibility_check_at = excluded.last_visibility_check_at,
		last_error = excluded.last_error,
		updated_at = excluded.updated_at`,
		record.PluginID,
		boolInt(record.Enabled),
		record.Status,
		record.Surface,
		record.Endpoint,
		string(capabilityJSON),
		record.VisibilityStatus,
		formatTime(record.LastProbeAt),
		formatTime(record.LastVisibilityCheckAt),
		record.LastError,
		formatTime(record.UpdatedAt),
	)
	if err != nil {
		return PluginRecord{}, fmt.Errorf("prismdb: upsert plugin: %w", err)
	}
	return db.FindPlugin(ctx, record.PluginID)
}

func (db *DB) SetPluginEnabled(ctx context.Context, pluginID string, enabled bool) (PluginRecord, error) {
	if db == nil || db.sql == nil {
		return PluginRecord{}, fmt.Errorf("prismdb: db unavailable")
	}
	pluginID = strings.ToLower(strings.TrimSpace(pluginID))
	if pluginID == "" {
		return PluginRecord{}, fmt.Errorf("prismdb: plugin_id required")
	}
	existing, err := db.FindPlugin(ctx, pluginID)
	switch {
	case err == nil:
		existing.Enabled = enabled
		return db.UpsertPlugin(ctx, existing)
	case err == sql.ErrNoRows:
		return db.UpsertPlugin(ctx, PluginRecord{
			PluginID: pluginID,
			Enabled:  enabled,
		})
	default:
		return PluginRecord{}, err
	}
}

func (db *DB) FindPlugin(ctx context.Context, pluginID string) (PluginRecord, error) {
	if db == nil || db.sql == nil {
		return PluginRecord{}, fmt.Errorf("prismdb: db unavailable")
	}
	pluginID = strings.ToLower(strings.TrimSpace(pluginID))
	if pluginID == "" {
		return PluginRecord{}, fmt.Errorf("prismdb: plugin_id required")
	}
	var record PluginRecord
	var enabled int
	var capabilityJSON, lastProbeAt, lastVisibilityCheckAt, updatedAt string
	err := db.sql.QueryRowContext(ctx, `SELECT
		plugin_id, enabled, status, surface, endpoint, capability_json, visibility_status,
		last_probe_at, last_visibility_check_at, last_error, updated_at
		FROM bridge_plugins
		WHERE plugin_id = ?`,
		pluginID,
	).Scan(
		&record.PluginID,
		&enabled,
		&record.Status,
		&record.Surface,
		&record.Endpoint,
		&capabilityJSON,
		&record.VisibilityStatus,
		&lastProbeAt,
		&lastVisibilityCheckAt,
		&record.LastError,
		&updatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return PluginRecord{}, sql.ErrNoRows
		}
		return PluginRecord{}, fmt.Errorf("prismdb: find plugin: %w", err)
	}
	record.Enabled = enabled != 0
	if strings.TrimSpace(capabilityJSON) != "" {
		_ = json.Unmarshal([]byte(capabilityJSON), &record.Capability)
	}
	record.LastProbeAt = parseTime(lastProbeAt)
	record.LastVisibilityCheckAt = parseTime(lastVisibilityCheckAt)
	record.UpdatedAt = parseTime(updatedAt)
	return record, nil
}

func (db *DB) ListPlugins(ctx context.Context) ([]PluginRecord, error) {
	if db == nil || db.sql == nil {
		return nil, fmt.Errorf("prismdb: db unavailable")
	}
	rows, err := db.sql.QueryContext(ctx, `SELECT
		plugin_id, enabled, status, surface, endpoint, capability_json, visibility_status,
		last_probe_at, last_visibility_check_at, last_error, updated_at
		FROM bridge_plugins
		ORDER BY plugin_id ASC`)
	if err != nil {
		return nil, fmt.Errorf("prismdb: list plugins: %w", err)
	}
	defer rows.Close()
	var out []PluginRecord
	for rows.Next() {
		var record PluginRecord
		var enabled int
		var capabilityJSON, lastProbeAt, lastVisibilityCheckAt, updatedAt string
		if err := rows.Scan(
			&record.PluginID,
			&enabled,
			&record.Status,
			&record.Surface,
			&record.Endpoint,
			&capabilityJSON,
			&record.VisibilityStatus,
			&lastProbeAt,
			&lastVisibilityCheckAt,
			&record.LastError,
			&updatedAt,
		); err != nil {
			return nil, fmt.Errorf("prismdb: scan plugin: %w", err)
		}
		record.Enabled = enabled != 0
		if strings.TrimSpace(capabilityJSON) != "" {
			_ = json.Unmarshal([]byte(capabilityJSON), &record.Capability)
		}
		record.LastProbeAt = parseTime(lastProbeAt)
		record.LastVisibilityCheckAt = parseTime(lastVisibilityCheckAt)
		record.UpdatedAt = parseTime(updatedAt)
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("prismdb: list plugins rows: %w", err)
	}
	return out, nil
}

type EventRecord struct {
	ID                  string         `json:"id"`
	SessionID           string         `json:"session_id,omitempty"`
	PrismConversationID string         `json:"prism_conversation_id,omitempty"`
	PluginID            string         `json:"plugin_id,omitempty"`
	NativeEventID       string         `json:"native_event_id,omitempty"`
	EventType           string         `json:"event_type"`
	Status              string         `json:"status,omitempty"`
	Summary             string         `json:"summary,omitempty"`
	Payload             map[string]any `json:"payload,omitempty"`
	CreatedAt           time.Time      `json:"created_at"`
}

type VisibilityCheckRecord struct {
	ID            string
	PluginID      string
	SessionID     string
	Marker        string
	Visible       bool
	Evidence      string
	FailureReason string
	CheckedAt     time.Time
}

type MessageReceiptRecord struct {
	PrismMessageID      string
	PrismConversationID string
	PluginID            string
	SessionID           string
	NativeMessageID     string
	Status              string
	Visible             bool
	CreatedAt           time.Time
	UpdatedAt           time.Time
	LastError           string
}

func (db *DB) UpsertSession(ctx context.Context, record SessionRecord) (SessionRecord, error) {
	if db == nil || db.sql == nil {
		return SessionRecord{}, fmt.Errorf("prismdb: db unavailable")
	}
	now := time.Now().UTC()
	if record.ID == "" {
		record.ID = uuid.NewString()
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	record.UpdatedAt = now
	nativeIdentity := nativeIdentity(record.NativeSessionID, record.NativeThreadID)
	_, err := db.sql.ExecContext(ctx, `INSERT INTO bridge_sessions (
		id, prism_conversation_id, plugin_id, cwd, native_session_id, native_thread_id,
		surface, endpoint, visibility_verified, visibility_evidence, created_at, updated_at, last_error, native_identity
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(prism_conversation_id, plugin_id) DO UPDATE SET
		cwd = excluded.cwd,
		native_session_id = excluded.native_session_id,
		native_thread_id = excluded.native_thread_id,
		surface = excluded.surface,
		endpoint = excluded.endpoint,
		visibility_verified = CASE WHEN excluded.native_identity = bridge_sessions.native_identity
			THEN MAX(bridge_sessions.visibility_verified, excluded.visibility_verified)
			ELSE excluded.visibility_verified END,
		visibility_evidence = CASE WHEN excluded.native_identity = bridge_sessions.native_identity
			AND excluded.visibility_evidence = '' THEN bridge_sessions.visibility_evidence
			ELSE excluded.visibility_evidence END,
		updated_at = excluded.updated_at,
		last_error = excluded.last_error,
		native_identity = excluded.native_identity`,
		record.ID,
		record.PrismConversationID,
		record.PluginID,
		record.Cwd,
		record.NativeSessionID,
		record.NativeThreadID,
		record.Surface,
		record.Endpoint,
		boolInt(record.VisibilityVerified),
		record.VisibilityEvidence,
		formatTime(record.CreatedAt),
		formatTime(record.UpdatedAt),
		record.LastError,
		nativeIdentity,
	)
	if err != nil {
		return SessionRecord{}, fmt.Errorf("prismdb: upsert session: %w", err)
	}
	stored, err := db.FindSession(ctx, record.PrismConversationID, record.PluginID)
	if err != nil {
		return SessionRecord{}, err
	}
	return stored, nil
}

// EnsureCanonicalSessionBinding returns the one opaque canonical conversation
// binding for a real native identity. It is used when a plugin discovers an
// existing desktop conversation, before that conversation is exposed remotely.
//
// A native identity is private to the Hub and Plugin. This method therefore
// never accepts a caller-provided conversation ID and never derives one from
// native data. INSERT OR IGNORE plus the unique native-identity index makes
// concurrent index refreshes converge on one durable binding.
func (db *DB) EnsureCanonicalSessionBinding(ctx context.Context, record SessionRecord) (SessionRecord, error) {
	if db == nil || db.sql == nil {
		return SessionRecord{}, fmt.Errorf("prismdb: db unavailable")
	}
	record.PluginID = strings.ToLower(strings.TrimSpace(record.PluginID))
	identity := nativeIdentity(record.NativeSessionID, record.NativeThreadID)
	if record.PluginID == "" || identity == "" {
		return SessionRecord{}, fmt.Errorf("prismdb: canonical binding requires plugin and native identity")
	}
	now := time.Now().UTC()
	created := SessionRecord{
		ID:                  uuid.NewString(),
		PrismConversationID: "conv_" + uuid.NewString(),
		PluginID:            record.PluginID,
		Cwd:                 strings.TrimSpace(record.Cwd),
		NativeSessionID:     strings.TrimSpace(record.NativeSessionID),
		NativeThreadID:      strings.TrimSpace(record.NativeThreadID),
		Surface:             strings.TrimSpace(record.Surface),
		Endpoint:            strings.TrimSpace(record.Endpoint),
		VisibilityVerified:  record.VisibilityVerified,
		VisibilityEvidence:  strings.TrimSpace(record.VisibilityEvidence),
		CreatedAt:           now,
		UpdatedAt:           now,
	}
	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return SessionRecord{}, fmt.Errorf("prismdb: begin canonical binding: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO bridge_sessions (
		id, prism_conversation_id, plugin_id, cwd, native_session_id, native_thread_id,
		surface, endpoint, visibility_verified, visibility_evidence, created_at, updated_at, last_error, native_identity
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', ?)`,
		created.ID, created.PrismConversationID, created.PluginID, created.Cwd, created.NativeSessionID,
		created.NativeThreadID, created.Surface, created.Endpoint, boolInt(created.VisibilityVerified),
		created.VisibilityEvidence, formatTime(created.CreatedAt), formatTime(created.UpdatedAt), identity)
	if err != nil {
		return SessionRecord{}, fmt.Errorf("prismdb: insert canonical binding: %w", err)
	}
	stored, found, err := findSessionByNativeIdentityTx(ctx, tx, record.PluginID, identity)
	if err != nil {
		return SessionRecord{}, err
	}
	if !found {
		return SessionRecord{}, fmt.Errorf("prismdb: canonical binding was not stored")
	}
	if needsCanonicalConversationID(stored.PrismConversationID) {
		// Legacy synthetic and pending identifiers embed or imply local identity.
		// They cannot remain an external conversation identity after this contract.
		canonicalID := "conv_" + uuid.NewString()
		if _, err := tx.ExecContext(ctx, `UPDATE bridge_sessions
			SET prism_conversation_id = ?, updated_at = ?
			WHERE id = ?`, canonicalID, formatTime(now), stored.ID); err != nil {
			return SessionRecord{}, fmt.Errorf("prismdb: rotate legacy conversation binding: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE bridge_message_receipts
			SET prism_conversation_id = ?, updated_at = ?
			WHERE plugin_id = ? AND prism_conversation_id = ?`, canonicalID, formatTime(now), record.PluginID, stored.PrismConversationID); err != nil {
			return SessionRecord{}, fmt.Errorf("prismdb: rotate legacy receipts: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE bridge_events
			SET prism_conversation_id = ?
			WHERE plugin_id = ? AND prism_conversation_id = ?`, canonicalID, record.PluginID, stored.PrismConversationID); err != nil {
			return SessionRecord{}, fmt.Errorf("prismdb: rotate legacy events: %w", err)
		}
		stored.PrismConversationID = canonicalID
		stored.UpdatedAt = now
	}
	// A discovery refresh can enrich local binding metadata but must never
	// replace the canonical identity or wipe fields the plugin did not provide.
	if _, err := tx.ExecContext(ctx, `UPDATE bridge_sessions SET
		cwd = CASE WHEN ? <> '' THEN ? ELSE cwd END,
		native_session_id = CASE WHEN ? <> '' THEN ? ELSE native_session_id END,
		native_thread_id = CASE WHEN ? <> '' THEN ? ELSE native_thread_id END,
		surface = CASE WHEN ? <> '' THEN ? ELSE surface END,
		endpoint = CASE WHEN ? <> '' THEN ? ELSE endpoint END,
		visibility_verified = CASE WHEN ? = 1 THEN 1 ELSE visibility_verified END,
		visibility_evidence = CASE WHEN ? <> '' THEN ? ELSE visibility_evidence END,
		updated_at = ?
		WHERE id = ?`,
		created.Cwd, created.Cwd,
		created.NativeSessionID, created.NativeSessionID,
		created.NativeThreadID, created.NativeThreadID,
		created.Surface, created.Surface,
		created.Endpoint, created.Endpoint,
		boolInt(created.VisibilityVerified),
		created.VisibilityEvidence, created.VisibilityEvidence,
		formatTime(now), stored.ID); err != nil {
		return SessionRecord{}, fmt.Errorf("prismdb: refresh canonical binding: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return SessionRecord{}, fmt.Errorf("prismdb: commit canonical binding: %w", err)
	}
	stored.Cwd = firstNonEmptyString(created.Cwd, stored.Cwd)
	stored.NativeSessionID = firstNonEmptyString(created.NativeSessionID, stored.NativeSessionID)
	stored.NativeThreadID = firstNonEmptyString(created.NativeThreadID, stored.NativeThreadID)
	stored.Surface = firstNonEmptyString(created.Surface, stored.Surface)
	stored.Endpoint = firstNonEmptyString(created.Endpoint, stored.Endpoint)
	if created.VisibilityVerified {
		stored.VisibilityVerified = true
	}
	stored.VisibilityEvidence = firstNonEmptyString(created.VisibilityEvidence, stored.VisibilityEvidence)
	stored.UpdatedAt = now
	return stored, nil
}

func needsCanonicalConversationID(conversationID string) bool {
	conversationID = strings.TrimSpace(conversationID)
	return strings.HasPrefix(conversationID, "session:") || strings.HasPrefix(conversationID, "pending-")
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

// BeginStartOperation records (or resumes) a private first-message operation.
// A completed operation is never reset: retries must return its original
// canonical conversation instead of invoking the native product again.
func (db *DB) BeginStartOperation(ctx context.Context, pluginID, prismMessageID, requestID string) (StartOperationRecord, error) {
	if db == nil || db.sql == nil {
		return StartOperationRecord{}, fmt.Errorf("prismdb: db unavailable")
	}
	pluginID = strings.ToLower(strings.TrimSpace(pluginID))
	prismMessageID = strings.TrimSpace(prismMessageID)
	if pluginID == "" || prismMessageID == "" {
		return StartOperationRecord{}, fmt.Errorf("prismdb: plugin_id and prism message id required")
	}
	now := time.Now().UTC()
	_, err := db.sql.ExecContext(ctx, `INSERT INTO bridge_start_operations (
		plugin_id, prism_message_id, request_id, status, created_at, updated_at
	) VALUES (?, ?, ?, 'starting', ?, ?)
	ON CONFLICT(plugin_id, prism_message_id) DO UPDATE SET
		request_id = CASE WHEN bridge_start_operations.status = 'completed' THEN bridge_start_operations.request_id ELSE excluded.request_id END,
		status = CASE WHEN bridge_start_operations.status = 'completed' THEN bridge_start_operations.status ELSE 'starting' END,
		last_error = CASE WHEN bridge_start_operations.status = 'completed' THEN bridge_start_operations.last_error ELSE '' END,
		updated_at = CASE WHEN bridge_start_operations.status = 'completed' THEN bridge_start_operations.updated_at ELSE excluded.updated_at END`,
		pluginID, prismMessageID, strings.TrimSpace(requestID), formatTime(now), formatTime(now))
	if err != nil {
		return StartOperationRecord{}, fmt.Errorf("prismdb: begin start operation: %w", err)
	}
	return db.FindStartOperation(ctx, pluginID, prismMessageID)
}

func (db *DB) FindStartOperation(ctx context.Context, pluginID, prismMessageID string) (StartOperationRecord, error) {
	if db == nil || db.sql == nil {
		return StartOperationRecord{}, fmt.Errorf("prismdb: db unavailable")
	}
	var record StartOperationRecord
	var createdAt, updatedAt string
	err := db.sql.QueryRowContext(ctx, `SELECT
		plugin_id, prism_message_id, request_id, status, prism_conversation_id,
		native_message_id, last_error, created_at, updated_at
		FROM bridge_start_operations
		WHERE plugin_id = ? AND prism_message_id = ?`,
		strings.ToLower(strings.TrimSpace(pluginID)), strings.TrimSpace(prismMessageID)).Scan(
		&record.PluginID,
		&record.PrismMessageID,
		&record.RequestID,
		&record.Status,
		&record.PrismConversationID,
		&record.NativeMessageID,
		&record.LastError,
		&createdAt,
		&updatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return StartOperationRecord{}, sql.ErrNoRows
		}
		return StartOperationRecord{}, fmt.Errorf("prismdb: find start operation: %w", err)
	}
	record.CreatedAt = parseTime(createdAt)
	record.UpdatedAt = parseTime(updatedAt)
	return record, nil
}

// CompleteStartOperation atomically establishes the opaque canonical binding,
// persists the first-message receipt, and marks the idempotency operation done.
func (db *DB) CompleteStartOperation(ctx context.Context, operation StartOperationRecord, session SessionRecord, nativeMessageID string) (StartOperationRecord, SessionRecord, error) {
	if db == nil || db.sql == nil {
		return StartOperationRecord{}, SessionRecord{}, fmt.Errorf("prismdb: db unavailable")
	}
	operation.PluginID = strings.ToLower(strings.TrimSpace(operation.PluginID))
	operation.PrismMessageID = strings.TrimSpace(operation.PrismMessageID)
	identity := nativeIdentity(session.NativeSessionID, session.NativeThreadID)
	if operation.PluginID == "" || operation.PrismMessageID == "" || identity == "" {
		return StartOperationRecord{}, SessionRecord{}, fmt.Errorf("prismdb: complete start requires plugin, message, and native identity")
	}
	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return StartOperationRecord{}, SessionRecord{}, fmt.Errorf("prismdb: begin complete start: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	stored, found, err := findSessionByNativeIdentityTx(ctx, tx, operation.PluginID, identity)
	if err != nil {
		return StartOperationRecord{}, SessionRecord{}, err
	}
	now := time.Now().UTC()
	if !found {
		stored = session
		stored.ID = uuid.NewString()
		stored.PrismConversationID = "conv_" + uuid.NewString()
		stored.PluginID = operation.PluginID
		stored.CreatedAt = now
		stored.UpdatedAt = now
		_, err = tx.ExecContext(ctx, `INSERT INTO bridge_sessions (
			id, prism_conversation_id, plugin_id, cwd, native_session_id, native_thread_id,
			surface, endpoint, visibility_verified, visibility_evidence, created_at, updated_at, last_error, native_identity
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			stored.ID, stored.PrismConversationID, stored.PluginID, stored.Cwd, stored.NativeSessionID,
			stored.NativeThreadID, stored.Surface, stored.Endpoint, boolInt(stored.VisibilityVerified),
			stored.VisibilityEvidence, formatTime(stored.CreatedAt), formatTime(stored.UpdatedAt), stored.LastError, identity)
		if err != nil {
			return StartOperationRecord{}, SessionRecord{}, fmt.Errorf("prismdb: insert start binding: %w", err)
		}
	}
	// Reverse discovery can establish the canonical binding before the first
	// message is verified. Promote that same binding atomically with the receipt.
	if session.VisibilityVerified {
		stored.VisibilityVerified = true
		if session.VisibilityEvidence != "" {
			stored.VisibilityEvidence = session.VisibilityEvidence
		}
		stored.UpdatedAt = now
		if _, err := tx.ExecContext(ctx, `UPDATE bridge_sessions SET
			visibility_verified = 1, visibility_evidence = ?, updated_at = ? WHERE id = ?`,
			stored.VisibilityEvidence, formatTime(now), stored.ID); err != nil {
			return StartOperationRecord{}, SessionRecord{}, fmt.Errorf("prismdb: verify start binding: %w", err)
		}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO bridge_message_receipts (
		prism_message_id, prism_conversation_id, plugin_id, session_id, native_message_id,
		status, visible, created_at, updated_at, last_error
	) VALUES (?, ?, ?, ?, ?, 'visible', 1, ?, ?, '')
	ON CONFLICT(plugin_id, prism_message_id) DO UPDATE SET
		prism_conversation_id = excluded.prism_conversation_id,
		session_id = excluded.session_id,
		native_message_id = excluded.native_message_id,
		status = excluded.status,
		visible = excluded.visible,
		updated_at = excluded.updated_at,
		last_error = ''`,
		operation.PrismMessageID, stored.PrismConversationID, operation.PluginID, stored.ID,
		strings.TrimSpace(nativeMessageID), formatTime(now), formatTime(now))
	if err != nil {
		return StartOperationRecord{}, SessionRecord{}, fmt.Errorf("prismdb: persist start receipt: %w", err)
	}
	_, err = tx.ExecContext(ctx, `UPDATE bridge_start_operations
		SET status = 'completed', prism_conversation_id = ?, native_message_id = ?, last_error = '', updated_at = ?
		WHERE plugin_id = ? AND prism_message_id = ?`,
		stored.PrismConversationID, strings.TrimSpace(nativeMessageID), formatTime(now), operation.PluginID, operation.PrismMessageID)
	if err != nil {
		return StartOperationRecord{}, SessionRecord{}, fmt.Errorf("prismdb: complete start operation: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return StartOperationRecord{}, SessionRecord{}, fmt.Errorf("prismdb: commit complete start: %w", err)
	}
	completed := operation
	completed.Status = "completed"
	completed.PrismConversationID = stored.PrismConversationID
	completed.NativeMessageID = strings.TrimSpace(nativeMessageID)
	completed.UpdatedAt = now
	return completed, stored, nil
}

func findSessionByNativeIdentityTx(ctx context.Context, tx *sql.Tx, pluginID, identity string) (SessionRecord, bool, error) {
	var record SessionRecord
	var visibilityVerified int
	var createdAt, updatedAt string
	err := tx.QueryRowContext(ctx, `SELECT
		id, prism_conversation_id, plugin_id, cwd, native_session_id, native_thread_id,
		surface, endpoint, visibility_verified, visibility_evidence, created_at, updated_at, last_error
		FROM bridge_sessions
		WHERE plugin_id = ? AND native_identity = ?`, pluginID, identity).Scan(
		&record.ID, &record.PrismConversationID, &record.PluginID, &record.Cwd, &record.NativeSessionID,
		&record.NativeThreadID, &record.Surface, &record.Endpoint, &visibilityVerified,
		&record.VisibilityEvidence, &createdAt, &updatedAt, &record.LastError,
	)
	if err == sql.ErrNoRows {
		return SessionRecord{}, false, nil
	}
	if err != nil {
		return SessionRecord{}, false, fmt.Errorf("prismdb: find native binding: %w", err)
	}
	record.VisibilityVerified = visibilityVerified != 0
	record.CreatedAt = parseTime(createdAt)
	record.UpdatedAt = parseTime(updatedAt)
	return record, true, nil
}

func nativeIdentity(nativeSessionID, nativeThreadID string) string {
	if nativeThreadID = strings.TrimSpace(nativeThreadID); nativeThreadID != "" {
		return "thread:" + nativeThreadID
	}
	if nativeSessionID = strings.TrimSpace(nativeSessionID); nativeSessionID != "" {
		return "session:" + nativeSessionID
	}
	return ""
}

func (db *DB) FindSession(ctx context.Context, prismConversationID, pluginID string) (SessionRecord, error) {
	if db == nil || db.sql == nil {
		return SessionRecord{}, fmt.Errorf("prismdb: db unavailable")
	}
	var record SessionRecord
	var visibilityVerified int
	var createdAt, updatedAt string
	err := db.sql.QueryRowContext(ctx, `SELECT
		id, prism_conversation_id, plugin_id, cwd, native_session_id, native_thread_id,
		surface, endpoint, visibility_verified, visibility_evidence, created_at, updated_at, last_error
		FROM bridge_sessions
		WHERE prism_conversation_id = ? AND plugin_id = ?`,
		prismConversationID, pluginID,
	).Scan(
		&record.ID,
		&record.PrismConversationID,
		&record.PluginID,
		&record.Cwd,
		&record.NativeSessionID,
		&record.NativeThreadID,
		&record.Surface,
		&record.Endpoint,
		&visibilityVerified,
		&record.VisibilityEvidence,
		&createdAt,
		&updatedAt,
		&record.LastError,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return SessionRecord{}, sql.ErrNoRows
		}
		return SessionRecord{}, fmt.Errorf("prismdb: find session: %w", err)
	}
	record.VisibilityVerified = visibilityVerified != 0
	record.CreatedAt = parseTime(createdAt)
	record.UpdatedAt = parseTime(updatedAt)
	return record, nil
}

func (db *DB) ListSessions(ctx context.Context, pluginID string) ([]SessionRecord, error) {
	if db == nil || db.sql == nil {
		return nil, fmt.Errorf("prismdb: db unavailable")
	}
	pluginID = strings.TrimSpace(pluginID)
	query := `SELECT
		id, prism_conversation_id, plugin_id, cwd, native_session_id, native_thread_id,
		surface, endpoint, visibility_verified, visibility_evidence, created_at, updated_at, last_error
		FROM bridge_sessions`
	var args []any
	if pluginID != "" {
		query += ` WHERE plugin_id = ?`
		args = append(args, pluginID)
	}
	query += ` ORDER BY updated_at DESC`
	rows, err := db.sql.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("prismdb: list sessions: %w", err)
	}
	defer rows.Close()
	var out []SessionRecord
	for rows.Next() {
		var record SessionRecord
		var visibilityVerified int
		var createdAt, updatedAt string
		if err := rows.Scan(
			&record.ID,
			&record.PrismConversationID,
			&record.PluginID,
			&record.Cwd,
			&record.NativeSessionID,
			&record.NativeThreadID,
			&record.Surface,
			&record.Endpoint,
			&visibilityVerified,
			&record.VisibilityEvidence,
			&createdAt,
			&updatedAt,
			&record.LastError,
		); err != nil {
			return nil, fmt.Errorf("prismdb: scan session: %w", err)
		}
		record.VisibilityVerified = visibilityVerified != 0
		record.CreatedAt = parseTime(createdAt)
		record.UpdatedAt = parseTime(updatedAt)
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("prismdb: list sessions rows: %w", err)
	}
	return out, nil
}

func (db *DB) InsertEvent(ctx context.Context, record EventRecord) (EventRecord, error) {
	if db == nil || db.sql == nil {
		return EventRecord{}, fmt.Errorf("prismdb: db unavailable")
	}
	if record.ID == "" {
		record.ID = uuid.NewString()
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now().UTC()
	}
	payload, err := json.Marshal(record.Payload)
	if err != nil {
		return EventRecord{}, fmt.Errorf("prismdb: marshal event payload: %w", err)
	}
	_, err = db.sql.ExecContext(ctx, `INSERT INTO bridge_events (
		id, session_id, prism_conversation_id, plugin_id, native_event_id,
		event_type, status, summary, payload_json, created_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		record.ID,
		record.SessionID,
		record.PrismConversationID,
		record.PluginID,
		record.NativeEventID,
		record.EventType,
		record.Status,
		record.Summary,
		string(payload),
		formatTime(record.CreatedAt),
	)
	if err != nil {
		return EventRecord{}, fmt.Errorf("prismdb: insert event: %w", err)
	}
	return record, nil
}

// InsertNativeEventOnce persists a plugin event with a stable ID. Replayed
// native events must not be forwarded to Realtime a second time.
func (db *DB) InsertNativeEventOnce(ctx context.Context, record EventRecord) (bool, error) {
	if db == nil || db.sql == nil {
		return false, fmt.Errorf("prismdb: db unavailable")
	}
	if record.ID == "" || record.NativeEventID == "" {
		return false, fmt.Errorf("prismdb: stable native event ID required")
	}
	var existing string
	err := db.sql.QueryRowContext(ctx, `SELECT id FROM bridge_events WHERE session_id = ? AND native_event_id = ? LIMIT 1`, record.SessionID, record.NativeEventID).Scan(&existing)
	if err == nil {
		return false, nil
	}
	if err != sql.ErrNoRows {
		return false, fmt.Errorf("prismdb: find native event: %w", err)
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now().UTC()
	}
	payload, err := json.Marshal(record.Payload)
	if err != nil {
		return false, fmt.Errorf("prismdb: marshal native event payload: %w", err)
	}
	result, err := db.sql.ExecContext(ctx, `INSERT INTO bridge_events (
		id, session_id, prism_conversation_id, plugin_id, native_event_id,
		event_type, status, summary, payload_json, created_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO NOTHING`,
		record.ID, record.SessionID, record.PrismConversationID, record.PluginID,
		record.NativeEventID, record.EventType, record.Status, record.Summary,
		string(payload), formatTime(record.CreatedAt))
	if err != nil {
		return false, fmt.Errorf("prismdb: insert native event: %w", err)
	}
	rows, err := result.RowsAffected()
	return rows == 1, err
}

// NativeEventDelivered reports whether a durable Hook event has a recorded
// Realtime server receipt. It does not prove that Panel consumed the event.
func (db *DB) NativeEventDelivered(ctx context.Context, sessionID, nativeEventID string) (bool, error) {
	if db == nil || db.sql == nil {
		return false, fmt.Errorf("prismdb: db unavailable")
	}
	var exists int
	err := db.sql.QueryRowContext(ctx, `SELECT 1 FROM bridge_native_event_delivery WHERE session_id = ? AND native_event_id = ?`, sessionID, nativeEventID).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

func (db *DB) MarkNativeEventDelivered(ctx context.Context, sessionID, nativeEventID string) error {
	if db == nil || db.sql == nil {
		return fmt.Errorf("prismdb: db unavailable")
	}
	if sessionID == "" || nativeEventID == "" {
		return fmt.Errorf("prismdb: native event identity required")
	}
	_, err := db.sql.ExecContext(ctx, `INSERT INTO bridge_native_event_delivery(session_id, native_event_id, delivered_at)
		VALUES(?, ?, ?) ON CONFLICT(session_id, native_event_id) DO NOTHING`, sessionID, nativeEventID, formatTime(time.Now().UTC()))
	return err
}

// PruneEvents 删除保留窗口之外或超过最新 maxEvents 条的诊断桥接事件。
// 会话和消息回执属于运行时状态而非历史记录，因此会保留。
func (db *DB) PruneEvents(ctx context.Context, retainSince time.Time, maxEvents int) (int64, error) {
	if db == nil || db.sql == nil {
		return 0, fmt.Errorf("prismdb: db unavailable")
	}
	if retainSince.IsZero() {
		return 0, fmt.Errorf("prismdb: event retention cutoff required")
	}
	if maxEvents <= 0 {
		maxEvents = DefaultEventRetentionLimit
	}
	result, err := db.sql.ExecContext(ctx, `DELETE FROM bridge_events
		WHERE created_at < ? OR id IN (
			SELECT id FROM bridge_events
			ORDER BY created_at DESC
			LIMIT -1 OFFSET ?
		)`, formatTime(retainSince.UTC()), maxEvents)
	if err != nil {
		return 0, fmt.Errorf("prismdb: prune events: %w", err)
	}
	deleted, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("prismdb: count pruned events: %w", err)
	}
	return deleted, nil
}

func (db *DB) ListRecentEvents(ctx context.Context, limit int) ([]EventRecord, error) {
	if db == nil || db.sql == nil {
		return nil, fmt.Errorf("prismdb: db unavailable")
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}
	rows, err := db.sql.QueryContext(ctx, `SELECT
		id, session_id, prism_conversation_id, plugin_id, native_event_id,
		event_type, status, summary, payload_json, created_at
		FROM bridge_events
		ORDER BY created_at DESC
		LIMIT ?`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("prismdb: list recent events: %w", err)
	}
	defer rows.Close()
	var out []EventRecord
	for rows.Next() {
		var record EventRecord
		var payloadJSON string
		var createdAt string
		if err := rows.Scan(
			&record.ID,
			&record.SessionID,
			&record.PrismConversationID,
			&record.PluginID,
			&record.NativeEventID,
			&record.EventType,
			&record.Status,
			&record.Summary,
			&payloadJSON,
			&createdAt,
		); err != nil {
			return nil, fmt.Errorf("prismdb: scan recent event: %w", err)
		}
		if strings.TrimSpace(payloadJSON) != "" {
			_ = json.Unmarshal([]byte(payloadJSON), &record.Payload)
		}
		record.CreatedAt = parseTime(createdAt)
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("prismdb: list recent events rows: %w", err)
	}
	return out, nil
}

func (db *DB) ListConversationEvents(ctx context.Context, prismConversationID, pluginID string, limit int) ([]EventRecord, error) {
	if db == nil || db.sql == nil {
		return nil, fmt.Errorf("prismdb: db unavailable")
	}
	prismConversationID = strings.TrimSpace(prismConversationID)
	pluginID = strings.TrimSpace(pluginID)
	if prismConversationID == "" {
		return nil, fmt.Errorf("prismdb: prism conversation id required")
	}
	if limit <= 0 {
		limit = 200
	}
	if limit > 2000 {
		limit = 2000
	}
	query := `SELECT
		id, session_id, prism_conversation_id, plugin_id, native_event_id,
		event_type, status, summary, payload_json, created_at
		FROM bridge_events
		WHERE prism_conversation_id = ?`
	args := []any{prismConversationID}
	if pluginID != "" {
		query += ` AND plugin_id = ?`
		args = append(args, pluginID)
	}
	query += ` ORDER BY created_at ASC LIMIT ?`
	args = append(args, limit)
	rows, err := db.sql.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("prismdb: list conversation events: %w", err)
	}
	defer rows.Close()
	var out []EventRecord
	for rows.Next() {
		var record EventRecord
		var payloadJSON string
		var createdAt string
		if err := rows.Scan(
			&record.ID,
			&record.SessionID,
			&record.PrismConversationID,
			&record.PluginID,
			&record.NativeEventID,
			&record.EventType,
			&record.Status,
			&record.Summary,
			&payloadJSON,
			&createdAt,
		); err != nil {
			return nil, fmt.Errorf("prismdb: scan conversation event: %w", err)
		}
		if strings.TrimSpace(payloadJSON) != "" {
			_ = json.Unmarshal([]byte(payloadJSON), &record.Payload)
		}
		record.CreatedAt = parseTime(createdAt)
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("prismdb: list conversation events rows: %w", err)
	}
	return out, nil
}

func (db *DB) InsertVisibilityCheck(ctx context.Context, record VisibilityCheckRecord) (VisibilityCheckRecord, error) {
	if db == nil || db.sql == nil {
		return VisibilityCheckRecord{}, fmt.Errorf("prismdb: db unavailable")
	}
	if record.ID == "" {
		record.ID = uuid.NewString()
	}
	if record.CheckedAt.IsZero() {
		record.CheckedAt = time.Now().UTC()
	}
	_, err := db.sql.ExecContext(ctx, `INSERT INTO bridge_visibility_checks (
		id, plugin_id, session_id, marker, visible, evidence, failure_reason, checked_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		record.ID,
		record.PluginID,
		record.SessionID,
		record.Marker,
		boolInt(record.Visible),
		record.Evidence,
		record.FailureReason,
		formatTime(record.CheckedAt),
	)
	if err != nil {
		return VisibilityCheckRecord{}, fmt.Errorf("prismdb: insert visibility check: %w", err)
	}
	return record, nil
}

// LatestVisibilityCheck returns the most recent visibility check recorded for
// the given plugin_id. It returns sql.ErrNoRows when no check exists, which the
// caller should treat as "not verified". The query uses the
// bridge_visibility_plugin_idx(plugin_id, checked_at) index.
func (db *DB) LatestVisibilityCheck(ctx context.Context, pluginID string) (VisibilityCheckRecord, error) {
	if db == nil || db.sql == nil {
		return VisibilityCheckRecord{}, fmt.Errorf("prismdb: db unavailable")
	}
	var record VisibilityCheckRecord
	var visible int
	var checkedAt string
	err := db.sql.QueryRowContext(ctx, `SELECT
		id, plugin_id, session_id, marker, visible, evidence, failure_reason, checked_at
		FROM bridge_visibility_checks
		WHERE plugin_id = ?
		ORDER BY checked_at DESC
		LIMIT 1`,
		pluginID,
	).Scan(
		&record.ID,
		&record.PluginID,
		&record.SessionID,
		&record.Marker,
		&visible,
		&record.Evidence,
		&record.FailureReason,
		&checkedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return VisibilityCheckRecord{}, sql.ErrNoRows
		}
		return VisibilityCheckRecord{}, fmt.Errorf("prismdb: latest visibility check: %w", err)
	}
	record.Visible = visible != 0
	record.CheckedAt = parseTime(checkedAt)
	return record, nil
}

func (db *DB) FindMessageReceipt(ctx context.Context, pluginID, prismMessageID string) (MessageReceiptRecord, error) {
	if db == nil || db.sql == nil {
		return MessageReceiptRecord{}, fmt.Errorf("prismdb: db unavailable")
	}
	pluginID = strings.ToLower(strings.TrimSpace(pluginID))
	prismMessageID = strings.TrimSpace(prismMessageID)
	if pluginID == "" || prismMessageID == "" {
		return MessageReceiptRecord{}, fmt.Errorf("prismdb: plugin_id and prism message id required")
	}
	var record MessageReceiptRecord
	var visible int
	var createdAt, updatedAt string
	err := db.sql.QueryRowContext(ctx, `SELECT
		prism_message_id, prism_conversation_id, plugin_id, session_id, native_message_id,
		status, visible, created_at, updated_at, last_error
		FROM bridge_message_receipts
		WHERE plugin_id = ? AND prism_message_id = ?`,
		pluginID, prismMessageID,
	).Scan(
		&record.PrismMessageID,
		&record.PrismConversationID,
		&record.PluginID,
		&record.SessionID,
		&record.NativeMessageID,
		&record.Status,
		&visible,
		&createdAt,
		&updatedAt,
		&record.LastError,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return MessageReceiptRecord{}, sql.ErrNoRows
		}
		return MessageReceiptRecord{}, fmt.Errorf("prismdb: find message receipt: %w", err)
	}
	record.Visible = visible != 0
	record.CreatedAt = parseTime(createdAt)
	record.UpdatedAt = parseTime(updatedAt)
	return record, nil
}

func (db *DB) UpsertMessageReceipt(ctx context.Context, record MessageReceiptRecord) (MessageReceiptRecord, error) {
	if db == nil || db.sql == nil {
		return MessageReceiptRecord{}, fmt.Errorf("prismdb: db unavailable")
	}
	record.PluginID = strings.ToLower(strings.TrimSpace(record.PluginID))
	record.PrismMessageID = strings.TrimSpace(record.PrismMessageID)
	if record.PluginID == "" || record.PrismMessageID == "" {
		return MessageReceiptRecord{}, fmt.Errorf("prismdb: plugin_id and prism message id required")
	}
	now := time.Now().UTC()
	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	record.UpdatedAt = now
	_, err := db.sql.ExecContext(ctx, `INSERT INTO bridge_message_receipts (
		prism_message_id, prism_conversation_id, plugin_id, session_id, native_message_id,
		status, visible, created_at, updated_at, last_error
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(plugin_id, prism_message_id) DO UPDATE SET
		prism_conversation_id = excluded.prism_conversation_id,
		session_id = excluded.session_id,
		native_message_id = excluded.native_message_id,
		status = excluded.status,
		visible = excluded.visible,
		updated_at = excluded.updated_at,
		last_error = excluded.last_error`,
		record.PrismMessageID,
		record.PrismConversationID,
		record.PluginID,
		record.SessionID,
		record.NativeMessageID,
		record.Status,
		boolInt(record.Visible),
		formatTime(record.CreatedAt),
		formatTime(record.UpdatedAt),
		record.LastError,
	)
	if err != nil {
		return MessageReceiptRecord{}, fmt.Errorf("prismdb: upsert message receipt: %w", err)
	}
	return db.FindMessageReceipt(ctx, record.PluginID, record.PrismMessageID)
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func parseTime(value string) time.Time {
	if value == "" {
		return time.Time{}
	}
	parsed, _ := time.Parse(time.RFC3339Nano, value)
	return parsed
}
