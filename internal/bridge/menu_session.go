package bridge

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

const menuSessionTTL = 2 * time.Minute

var errMenuSessionStale = errors.New("control_target_stale: menu session is no longer valid")

// MenuSessionEntry is a display-only choice returned by a Plugin after it has
// opened one native menu. EntryID is opaque to the Hub and clients.
type MenuSessionEntry struct {
	EntryID   string `json:"entry_id"`
	Label     string `json:"label"`
	Available bool   `json:"available"`
	Current   bool   `json:"current"`
}

// MenuSessionGroup preserves native menu hierarchy for clients that can
// render it. Entries remain the authoritative apply capability because a
// selected entry may only be used with its parent menu session.
type MenuSessionGroup struct {
	GroupID string             `json:"group_id"`
	Label   string             `json:"label"`
	Entries []MenuSessionEntry `json:"entries"`
}

// MenuSession is intentionally transient. It binds a Plugin-owned menu token
// to the exact Prism subject and owner that caused the native menu to open.
// It must never be persisted or published through the state projection.
type MenuSession struct {
	ID                  string         `json:"menu_session_id"`
	PluginMenuSessionID string         `json:"-"`
	PluginID            string         `json:"-"`
	SubjectKind         string         `json:"-"`
	SubjectID           string         `json:"-"`
	Owner               map[string]any `json:"owner"`
	ownerFingerprint    string
	Entries             []MenuSessionEntry `json:"entries"`
	Groups              []MenuSessionGroup `json:"groups,omitempty"`
	CreatedAt           time.Time          `json:"-"`
	ExpiresAt           time.Time          `json:"expires_at"`
}

type MenuSessionCreate struct {
	PluginMenuSessionID string
	PluginID            string
	SubjectKind         string
	SubjectID           string
	Owner               map[string]any
	Entries             []MenuSessionEntry
	Groups              []MenuSessionGroup
}

// MenuSessionStore is an in-memory, non-renewable capability cache. A Hub
// restart deliberately forgets all sessions, which is equivalent to stale.
type MenuSessionStore struct {
	mu       sync.Mutex
	sessions map[string]MenuSession
	now      func() time.Time
	newID    func() string
}

func NewMenuSessionStore() *MenuSessionStore {
	return &MenuSessionStore{
		sessions: map[string]MenuSession{},
		now:      time.Now,
		newID:    randomMenuSessionID,
	}
}

func (s *MenuSessionStore) Create(input MenuSessionCreate) (MenuSession, error) {
	if s == nil {
		return MenuSession{}, fmt.Errorf("menu session store unavailable")
	}
	input.PluginMenuSessionID = strings.TrimSpace(input.PluginMenuSessionID)
	input.PluginID = strings.TrimSpace(input.PluginID)
	input.SubjectKind = strings.TrimSpace(input.SubjectKind)
	input.SubjectID = strings.TrimSpace(input.SubjectID)
	ownerFingerprint, err := menuSessionOwnerFingerprint(input.Owner)
	if err != nil {
		return MenuSession{}, err
	}
	if input.PluginMenuSessionID == "" || input.PluginID == "" || input.SubjectID == "" || input.SubjectKind == "" || ownerFingerprint == "" {
		return MenuSession{}, fmt.Errorf("menu session requires plugin session, plugin, subject and owner")
	}
	entries := cloneMenuSessionEntries(input.Entries)
	if len(entries) == 0 {
		return MenuSession{}, fmt.Errorf("menu session requires entries")
	}
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if entry.EntryID == "" || entry.Label == "" {
			return MenuSession{}, fmt.Errorf("menu session entry requires entry_id and label")
		}
		if _, exists := seen[entry.EntryID]; exists {
			return MenuSession{}, fmt.Errorf("menu session entry_id must be unique")
		}
		seen[entry.EntryID] = struct{}{}
	}
	groups := cloneMenuSessionGroups(input.Groups)
	seenGroups := make(map[string]struct{}, len(groups))
	usedEntries := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		usedEntries[entry.EntryID] = struct{}{}
	}
	for _, group := range groups {
		if group.GroupID == "" || group.Label == "" || len(group.Entries) == 0 {
			return MenuSession{}, fmt.Errorf("menu session group requires group_id, label and entries")
		}
		if _, exists := seenGroups[group.GroupID]; exists {
			return MenuSession{}, fmt.Errorf("menu session group_id must be unique")
		}
		seenGroups[group.GroupID] = struct{}{}
		for _, entry := range group.Entries {
			if entry.EntryID == "" || entry.Label == "" {
				return MenuSession{}, fmt.Errorf("menu session group entry requires entry_id and label")
			}
			if _, exists := usedEntries[entry.EntryID]; !exists {
				return MenuSession{}, fmt.Errorf("menu session group entry is not present in session entries")
			}
		}
	}
	now := s.currentTime()
	session := MenuSession{
		ID:                  s.nextID(),
		PluginMenuSessionID: input.PluginMenuSessionID,
		PluginID:            input.PluginID,
		SubjectKind:         input.SubjectKind,
		SubjectID:           input.SubjectID,
		Owner:               cloneMenuSessionOwner(input.Owner),
		ownerFingerprint:    ownerFingerprint,
		Entries:             entries,
		Groups:              groups,
		CreatedAt:           now,
		ExpiresAt:           now.Add(menuSessionTTL),
	}
	if session.ID == "" {
		return MenuSession{}, fmt.Errorf("menu session id unavailable")
	}
	s.mu.Lock()
	s.purgeExpiredLocked(now)
	s.sessions[session.ID] = session
	s.mu.Unlock()
	return cloneMenuSession(session), nil
}

// Resolve verifies the full session binding. The caller provides whether a
// conversation is still foreground; draft callers pass true because draft
// identity is validated by their active-draft record instead.
func (s *MenuSessionStore) Resolve(id, pluginID, subjectKind, subjectID string, owner map[string]any, foreground bool) (MenuSession, error) {
	if s == nil {
		return MenuSession{}, errMenuSessionStale
	}
	fingerprint, err := menuSessionOwnerFingerprint(owner)
	if err != nil {
		return MenuSession{}, errMenuSessionStale
	}
	now := s.currentTime()
	s.mu.Lock()
	s.purgeExpiredLocked(now)
	session, ok := s.sessions[strings.TrimSpace(id)]
	if !ok || !foreground ||
		session.PluginID != strings.TrimSpace(pluginID) ||
		session.SubjectKind != strings.TrimSpace(subjectKind) ||
		session.SubjectID != strings.TrimSpace(subjectID) ||
		(fingerprint != "" && session.ownerFingerprint != fingerprint) {
		s.mu.Unlock()
		return MenuSession{}, errMenuSessionStale
	}
	s.mu.Unlock()
	return cloneMenuSession(session), nil
}

func (s *MenuSessionStore) DeleteForSubject(pluginID, subjectKind, subjectID string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, session := range s.sessions {
		if session.PluginID == strings.TrimSpace(pluginID) && session.SubjectKind == strings.TrimSpace(subjectKind) && session.SubjectID == strings.TrimSpace(subjectID) {
			delete(s.sessions, id)
		}
	}
}

func (s *MenuSessionStore) currentTime() time.Time {
	if s != nil && s.now != nil {
		return s.now().UTC()
	}
	return time.Now().UTC()
}

func (s *MenuSessionStore) nextID() string {
	if s != nil && s.newID != nil {
		return strings.TrimSpace(s.newID())
	}
	return randomMenuSessionID()
}

func (s *MenuSessionStore) purgeExpiredLocked(now time.Time) {
	for id, session := range s.sessions {
		if !now.Before(session.ExpiresAt) {
			delete(s.sessions, id)
		}
	}
}

func randomMenuSessionID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return ""
	}
	return "menu_" + hex.EncodeToString(value[:])
}

func menuSessionOwnerFingerprint(owner map[string]any) (string, error) {
	if len(owner) == 0 {
		return "", nil
	}
	encoded, err := json.Marshal(owner)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func cloneMenuSession(session MenuSession) MenuSession {
	session.Owner = cloneMenuSessionOwner(session.Owner)
	session.Entries = cloneMenuSessionEntries(session.Entries)
	session.Groups = cloneMenuSessionGroups(session.Groups)
	return session
}

func cloneMenuSessionOwner(owner map[string]any) map[string]any {
	if len(owner) == 0 {
		return nil
	}
	encoded, err := json.Marshal(owner)
	if err != nil {
		return nil
	}
	var cloned map[string]any
	if json.Unmarshal(encoded, &cloned) != nil {
		return nil
	}
	return cloned
}

func cloneMenuSessionEntries(entries []MenuSessionEntry) []MenuSessionEntry {
	if len(entries) == 0 {
		return nil
	}
	cloned := make([]MenuSessionEntry, len(entries))
	copy(cloned, entries)
	return cloned
}

func cloneMenuSessionGroups(groups []MenuSessionGroup) []MenuSessionGroup {
	if len(groups) == 0 {
		return nil
	}
	cloned := make([]MenuSessionGroup, len(groups))
	for index, group := range groups {
		cloned[index] = MenuSessionGroup{
			GroupID: group.GroupID,
			Label:   group.Label,
			Entries: cloneMenuSessionEntries(group.Entries),
		}
	}
	return cloned
}
