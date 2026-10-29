package bridge

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"rokid-prism-hub/internal/pluginruntime"
)

// DetailSnapshotStore keeps the latest canonical detail for a local Hub process.
// Realtime remains the cross-device cache; this store prevents local producers from
// rebuilding divergent detail payloads between watcher, control, and query paths.
type DetailSnapshotStore struct {
	mu                          sync.RWMutex
	items                       map[string]map[string]any
	foregroundByRuntime         map[string]string
	watcherUnavailableByRuntime map[string]bool
}

func NewDetailSnapshotStore() *DetailSnapshotStore {
	return &DetailSnapshotStore{
		items:                       map[string]map[string]any{},
		foregroundByRuntime:         map[string]string{},
		watcherUnavailableByRuntime: map[string]bool{},
	}
}

func (s *DetailSnapshotStore) GetForRuntime(pluginID, conversationID string) (map[string]any, bool) {
	if s == nil {
		return nil, false
	}
	key := detailSnapshotKey(pluginID, conversationID)
	if key == "" {
		return nil, false
	}
	s.mu.RLock()
	item, ok := s.items[key]
	pluginID = strings.TrimSpace(pluginID)
	foregroundConversationID := s.foregroundByRuntime[pluginID]
	watcherUnavailable := s.watcherUnavailableByRuntime[pluginID]
	s.mu.RUnlock()
	if !ok {
		return nil, false
	}
	cloned := cloneDetailMap(item)
	if watcherUnavailable {
		cloned["desktop_foreground"] = false
		cloned["detail_stale"] = true
		cloned["interactive_surface"] = nil
		failClosedTerminal(cloned, "plugin_watcher_unavailable")
	} else if foregroundConversationID != "" || detailSnapshotHasForegroundState(cloned) {
		isForeground := foregroundConversationID != "" && foregroundConversationID == strings.TrimSpace(conversationID)
		cloned["desktop_foreground"] = isForeground
		cloned["detail_stale"] = !isForeground
		if !isForeground {
			cloned["interactive_surface"] = nil
		}
	}
	return cloned, true
}

func (s *DetailSnapshotStore) PutForRuntime(pluginID string, snapshot map[string]any) {
	if s == nil || len(snapshot) == 0 {
		return
	}
	key := detailSnapshotKey(pluginID, stringFromAny(snapshot["conversation_id"]))
	if key == "" {
		return
	}
	s.mu.Lock()
	if s.items == nil {
		s.items = map[string]map[string]any{}
	}
	s.items[key] = cloneDetailMap(snapshot)
	if s.foregroundByRuntime == nil {
		s.foregroundByRuntime = map[string]string{}
	}
	if s.watcherUnavailableByRuntime == nil {
		s.watcherUnavailableByRuntime = map[string]bool{}
	}
	pluginID = strings.TrimSpace(pluginID)
	conversationID := strings.TrimSpace(stringFromAny(snapshot["conversation_id"]))
	if firstNonEmptyBool(snapshot["desktop_foreground"]) {
		s.foregroundByRuntime[pluginID] = conversationID
		delete(s.watcherUnavailableByRuntime, pluginID)
	} else if detailSnapshotHasForegroundState(snapshot) && s.foregroundByRuntime[pluginID] == conversationID {
		delete(s.foregroundByRuntime, pluginID)
	}
	s.mu.Unlock()
}

// MarkForegroundWatcherUnavailable retains last-known details for explicit
// stale display, but removes foreground truth until the next valid live detail.
func (s *DetailSnapshotStore) MarkForegroundWatcherUnavailable(pluginID string) {
	if s == nil {
		return
	}
	pluginID = strings.TrimSpace(pluginID)
	if pluginID == "" {
		return
	}
	s.mu.Lock()
	if s.watcherUnavailableByRuntime == nil {
		s.watcherUnavailableByRuntime = map[string]bool{}
	}
	s.watcherUnavailableByRuntime[pluginID] = true
	delete(s.foregroundByRuntime, pluginID)
	s.mu.Unlock()
}

// MarkRuntimeUnavailable revokes cached terminal control capabilities when a
// native Plugin subscription is lost. A later complete watcher detail restores
// the state; cached can_send/can_approve must never authorize an operation.
func (s *DetailSnapshotStore) MarkRuntimeUnavailable(pluginID string) []map[string]any {
	return s.markTerminalUnavailable(pluginID, "")
}

func (s *DetailSnapshotStore) MarkTerminalUnavailable(pluginID, conversationID string) []map[string]any {
	return s.markTerminalUnavailable(pluginID, conversationID)
}

func (s *DetailSnapshotStore) markTerminalUnavailable(pluginID, conversationID string) []map[string]any {
	if s == nil {
		return nil
	}
	pluginID = strings.TrimSpace(pluginID)
	conversationID = strings.TrimSpace(conversationID)
	if pluginID == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var changed []map[string]any
	for key, item := range s.items {
		if !strings.HasPrefix(key, pluginID+"\x00") || len(asMap(item["terminal"])) == 0 {
			continue
		}
		if conversationID != "" && strings.TrimSpace(stringFromAny(item["conversation_id"])) != conversationID {
			continue
		}
		copy := cloneDetailMap(item)
		failClosedTerminal(copy, "plugin_watcher_unavailable")
		s.items[key] = copy
		changed = append(changed, cloneDetailMap(copy))
	}
	return changed
}

func failClosedTerminal(snapshot map[string]any, reason string) {
	if len(asMap(snapshot["terminal"])) == 0 {
		return
	}
	snapshot["terminal"] = map[string]any{
		"status": "reconnecting", "input_owner": nil,
		"can_send": false, "can_interrupt": false, "can_approve": false,
		"reason": reason,
	}
}

func (s *DetailSnapshotStore) IsForegroundWatcherUnavailable(pluginID string) bool {
	if s == nil {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.watcherUnavailableByRuntime[strings.TrimSpace(pluginID)]
}

func (s *DetailSnapshotStore) ForegroundConversationID(pluginID string) string {
	if s == nil {
		return ""
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.foregroundByRuntime[strings.TrimSpace(pluginID)]
}

func (s *DetailSnapshotStore) IsForegroundForRuntime(pluginID, conversationID string) bool {
	if s == nil {
		return false
	}
	pluginID = strings.TrimSpace(pluginID)
	conversationID = strings.TrimSpace(conversationID)
	if pluginID == "" || conversationID == "" {
		return false
	}
	s.mu.RLock()
	current := s.foregroundByRuntime[pluginID]
	s.mu.RUnlock()
	return current != "" && current == conversationID
}

func (s *DetailSnapshotStore) ForegroundSnapshots() []map[string]any {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.foregroundByRuntime) == 0 {
		return nil
	}
	out := make([]map[string]any, 0, len(s.foregroundByRuntime))
	for pluginID, conversationID := range s.foregroundByRuntime {
		key := detailSnapshotKey(pluginID, conversationID)
		if key == "" {
			continue
		}
		item, ok := s.items[key]
		if !ok {
			continue
		}
		cloned := cloneDetailMap(item)
		cloned["desktop_foreground"] = true
		cloned["detail_stale"] = false
		out = append(out, cloned)
	}
	return out
}

// TerminalSnapshots includes native sessions whose live terminal state is
// available independently of desktop foreground ownership.
func (s *DetailSnapshotStore) TerminalSnapshots() []map[string]any {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []map[string]any
	for _, item := range s.items {
		if len(asMap(item["terminal"])) == 0 {
			continue
		}
		out = append(out, cloneDetailMap(item))
	}
	return out
}

// ResetPlugin removes every detail owned by one Plugin runtime. A runtime-mode
// replacement must not merge the new runtime's partial detail with controls,
// approvals, queues, or foreground state retained from the previous runtime.
func (s *DetailSnapshotStore) ResetPlugin(pluginID string) {
	if s == nil {
		return
	}
	pluginID = strings.TrimSpace(pluginID)
	if pluginID == "" {
		return
	}
	prefix := pluginID + "\x00"
	s.mu.Lock()
	for key := range s.items {
		if strings.HasPrefix(key, prefix) {
			delete(s.items, key)
		}
	}
	delete(s.foregroundByRuntime, pluginID)
	delete(s.watcherUnavailableByRuntime, pluginID)
	s.mu.Unlock()
}

func detailSnapshotKey(pluginID, conversationID string) string {
	pluginID = strings.TrimSpace(pluginID)
	conversationID = strings.TrimSpace(conversationID)
	if pluginID == "" || conversationID == "" {
		return ""
	}
	return pluginID + "\x00" + conversationID
}

func canonicalDetailSnapshot(base, live map[string]any, messagesSignature string) map[string]any {
	run := canonicalRunSnapshot(mergedDetailMap(asMap(base["run"]), asMap(live["run"])))
	approval := canonicalApprovalSnapshotValue(base, live, run)
	if approval != nil {
		run["status"] = "waiting_approval"
		run["approval_blocked"] = true
		run["approval_request_id"] = firstNonEmpty(
			stringFromAny(approval["approval_request_id"]),
			stringFromAny(run["approval_request_id"]),
		)
	}

	primaryAction := canonicalPrimaryAction(firstNonEmpty(
		stringFromAny(live["primary_action"]),
		stringFromAny(run["primary_action"]),
	))
	run["primary_action"] = primaryAction

	updatedAt := firstNonZeroInt64(
		int64Value(live["updated_at"]),
		int64Value(base["updated_at"]),
		time.Now().UnixMilli(),
	)
	out := map[string]any{
		"conversation_id":              firstNonEmpty(stringFromAny(live["conversation_id"]), stringFromAny(base["conversation_id"])),
		"plugin_id":                    firstNonEmpty(stringFromAny(live["plugin_id"]), stringFromAny(base["plugin_id"])),
		"title":                        canonicalConversationTitle(firstNonEmpty(stringFromAny(live["title"]), stringFromAny(base["title"])), firstNonEmpty(stringFromAny(live["conversation_id"]), stringFromAny(base["conversation_id"]))),
		"cwd":                          firstNonEmpty(stringFromAny(live["cwd"]), stringFromAny(base["cwd"])),
		"pinned":                       firstNonEmptyBool(live["pinned"], base["pinned"]),
		"context_window_total":         detailContextValue(base, live, "context_window_total"),
		"context_tokens_used":          detailContextValue(base, live, "context_tokens_used"),
		"context_window_usage_percent": detailContextValue(base, live, "context_window_usage_percent"),
		"context_window":               detailContextValue(base, live, "context_window"),
		"current_model":                fallbackCanonicalControlValue(base, live, "current_model"),
		"current_reasoning":            fallbackCanonicalControlValue(base, live, "current_reasoning"),
		"current_permission":           fallbackCanonicalControlValue(base, live, "current_permission"),
		"model_options":                fallbackCanonicalControlOptions(base, live, "model_options"),
		"reasoning_options":            fallbackCanonicalControlOptions(base, live, "reasoning_options"),
		"permission_options":           fallbackCanonicalControlOptions(base, live, "permission_options"),
		"interactive_controls":         fallbackCanonicalInteractiveControls(base, live),
		"interactive_surface":          fallbackCanonicalInteractiveSurface(base, live),
		"conversation_menu_available":  fallbackCanonicalBool(base, live, "conversation_menu_available"),
		"actions":                      fallbackCanonicalControlOptions(base, live, "actions"),
		"composer":                     fallbackCanonicalComposer(base, live),
		"queue":                        fallbackCanonicalQueue(base, live),
		"plan_mode":                    fallbackCanonicalPlanMode(base, live),
		"goal":                         fallbackCanonicalGoal(base, live),
		"approval":                     nullableDetailMap(approval),
		"primary_action":               primaryAction,
		"run":                          run,
		"messages_signature":           strings.TrimSpace(messagesSignature),
		"updated_at":                   updatedAt,
	}
	if value, exists := live["terminal"]; exists {
		out["terminal"] = canonicalTerminalSnapshot(value)
	} else if len(asMap(base["terminal"])) != 0 {
		// A new live detail that omits terminal state cannot inherit prior
		// authority to send, interrupt or approve.
		out["terminal"] = map[string]any{
			"status": "reconnecting", "input_owner": nil,
			"can_send": false, "can_interrupt": false, "can_approve": false,
			"reason": "terminal_state_unobserved",
		}
	}
	if detailSnapshotHasForegroundState(live) || detailSnapshotHasForegroundState(base) {
		out["desktop_foreground"] = firstNonEmptyBool(live["desktop_foreground"], base["desktop_foreground"])
		out["detail_stale"] = firstNonEmptyBool(live["detail_stale"], base["detail_stale"])
	}
	if out["messages_signature"] == "" {
		out["messages_signature"] = firstNonEmpty(stringFromAny(live["messages_signature"]), stringFromAny(base["messages_signature"]))
	}
	return out
}

func canonicalTerminalSnapshot(value any) map[string]any {
	input := asMap(value)
	status := strings.TrimSpace(stringFromAny(input["status"]))
	switch status {
	case "starting", "attached", "detached", "stopped", "reconnecting":
	default:
		status = "reconnecting"
	}
	owner := strings.TrimSpace(stringFromAny(input["input_owner"]))
	var inputOwner any
	if owner == "local" || owner == "panel" {
		inputOwner = owner
	}
	result := map[string]any{
		"status": status, "input_owner": inputOwner,
		"can_send":      firstNonEmptyBool(input["can_send"]),
		"can_interrupt": firstNonEmptyBool(input["can_interrupt"]),
		"can_approve":   firstNonEmptyBool(input["can_approve"]),
	}
	if status == "stopped" || status == "reconnecting" || len(input) == 0 {
		result["can_send"] = false
		result["can_interrupt"] = false
		result["can_approve"] = false
	}
	if reason := strings.TrimSpace(stringFromAny(input["reason"])); reason != "" {
		result["reason"] = reason
	}
	return result
}

func detailContextValue(base, live map[string]any, key string) string {
	if value := strings.TrimSpace(stringFromAny(live[key])); value != "" {
		return value
	}
	if value := strings.TrimSpace(stringFromAny(base[key])); value != "" {
		return value
	}
	if value := strings.TrimSpace(stringFromAny(asMap(live["metadata"])[key])); value != "" {
		return value
	}
	return strings.TrimSpace(stringFromAny(asMap(base["metadata"])[key]))
}

func detailSnapshotHasForegroundState(snapshot map[string]any) bool {
	if len(snapshot) == 0 {
		return false
	}
	return hasBoolLike(snapshot["desktop_foreground"]) || hasBoolLike(snapshot["detail_stale"])
}

func hasBoolLike(value any) bool {
	switch v := value.(type) {
	case bool:
		return true
	case string:
		text := strings.TrimSpace(strings.ToLower(v))
		return text == "true" || text == "1" || text == "yes" || text == "false" || text == "0" || text == "no"
	default:
		return false
	}
}

func mergedDetailMap(base, live map[string]any) map[string]any {
	if len(base) == 0 && len(live) == 0 {
		return map[string]any{}
	}
	out := cloneDetailMap(base)
	for key, value := range live {
		out[key] = value
	}
	return out
}

func fallbackCanonicalControlValue(base, live map[string]any, key string) any {
	if value, ok := live[key]; ok {
		return nullableDetailMap(canonicalControlValue(value))
	}
	return nullableDetailMap(canonicalControlValue(base[key]))
}

func fallbackCanonicalBool(base, live map[string]any, key string) bool {
	if _, exists := live[key]; exists {
		return firstNonEmptyBool(live[key])
	}
	return firstNonEmptyBool(base[key])
}

func fallbackCanonicalControlOptions(base, live map[string]any, key string) []map[string]any {
	if value, ok := live[key]; ok {
		return canonicalControlOptions(value)
	}
	return canonicalControlOptions(base[key])
}

func fallbackCanonicalInteractiveControls(base, live map[string]any) []map[string]any {
	value, exists := live["interactive_controls"]
	if !exists {
		value = base["interactive_controls"]
	}
	items := listOfMapsValue(value)
	out := make([]map[string]any, 0, len(items))
	seenControls := map[string]bool{}
	for _, item := range items {
		controlID := strings.TrimSpace(stringFromAny(item["control_id"]))
		displayLabel := strings.TrimSpace(stringFromAny(item["display_label"]))
		currentValue := strings.TrimSpace(stringFromAny(item["current_value"]))
		if controlID == "" || displayLabel == "" || currentValue == "" || seenControls[controlID] {
			continue
		}
		options := make([]map[string]any, 0)
		seenOptions := map[string]bool{}
		for _, option := range listOfMapsValue(item["options"]) {
			optionID := strings.TrimSpace(stringFromAny(option["option_id"]))
			label := strings.TrimSpace(stringFromAny(option["label"]))
			if optionID == "" || label == "" || seenOptions[optionID] {
				continue
			}
			seenOptions[optionID] = true
			options = append(options, map[string]any{
				"option_id": optionID,
				"label":     label,
				"available": !hasFalseValue(option, "available"),
			})
		}
		seenControls[controlID] = true
		canonical := map[string]any{
			"control_id":     controlID,
			"display_label":  displayLabel,
			"current_value":  currentValue,
			"menu_available": !hasFalseValue(item, "menu_available"),
		}
		if semanticKinds := canonicalInteractiveControlSemanticKinds(item["semantic_kinds"]); len(semanticKinds) > 0 {
			canonical["semantic_kinds"] = semanticKinds
		}
		// Closed controls intentionally have no option rows. Rows belong to a
		// short-lived menu_session opened by an explicit user tap.
		if len(options) > 0 {
			canonical["options"] = options
		}
		out = append(out, canonical)
	}
	return out
}

func canonicalInteractiveControlSemanticKinds(value any) []string {
	values := make([]string, 0)
	switch typed := value.(type) {
	case []string:
		values = append(values, typed...)
	case []any:
		for _, item := range typed {
			values = append(values, stringFromAny(item))
		}
	case string:
		values = append(values, typed)
	}
	seen := map[string]bool{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if (value != "model" && value != "reasoning" && value != "permission") || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}

// applyRuntimeProfile enforces one active wire representation for every
// standard control and conversation operation owned by the active runtime.
func applyRuntimeProfile(snapshot map[string]any, metadata pluginruntime.LocalMetadata) map[string]any {
	if len(snapshot) == 0 || (!metadata.RuntimeControlsDeclared && !metadata.RuntimeOperationsDeclared) {
		return snapshot
	}
	snapshot["runtime_mode"] = strings.TrimSpace(metadata.CurrentRuntimeMode)
	if metadata.RuntimeControlsDeclared {
		applyRuntimeControlProfile(snapshot, metadata.RuntimeControls)
	}
	if metadata.RuntimeOperationsDeclared {
		applyRuntimeOperationProfile(snapshot, metadata.RuntimeOperations)
	}
	return snapshot
}

// applyRuntimeControlProfile selects stable current/options fields or
// semantic-tagged interactive controls for model, reasoning, and permission.
func applyRuntimeControlProfile(snapshot map[string]any, profile pluginruntime.RuntimeControlProfile) {
	implementations := map[string]pluginruntime.RuntimeControlImplementation{
		"model":      profile.Model,
		"reasoning":  profile.Reasoning,
		"permission": profile.Permission,
	}
	for kind, implementation := range implementations {
		if implementation == pluginruntime.RuntimeControlStable {
			continue
		}
		snapshot["current_"+kind] = nil
		snapshot[kind+"_options"] = []map[string]any{}
	}

	controls := listOfMapsValue(snapshot["interactive_controls"])
	filteredControls := make([]map[string]any, 0, len(controls))
	for _, control := range controls {
		semanticKinds := canonicalInteractiveControlSemanticKinds(control["semantic_kinds"])
		if len(semanticKinds) == 0 {
			filteredControls = append(filteredControls, control)
			continue
		}
		allowed := true
		for _, kind := range semanticKinds {
			if implementations[kind] != pluginruntime.RuntimeControlInteractive {
				allowed = false
				break
			}
		}
		if allowed {
			filteredControls = append(filteredControls, control)
		}
	}
	snapshot["interactive_controls"] = filteredControls

	actions := listOfMapsValue(snapshot["actions"])
	filteredActions := make([]map[string]any, 0, len(actions))
	for _, action := range actions {
		kind := semanticKindForStableAction(stringFromAny(action["id"]))
		if kind == "" || implementations[kind] == pluginruntime.RuntimeControlStable {
			filteredActions = append(filteredActions, action)
		}
	}
	snapshot["actions"] = filteredActions
}

func semanticKindForStableAction(actionID string) string {
	switch strings.ToLower(strings.TrimSpace(actionID)) {
	case "model.switch", "model_switch":
		return "model"
	case "reasoning.switch", "reasoning_switch":
		return "reasoning"
	case "permission.switch", "permission_switch", "approval.switch", "approval_switch":
		return "permission"
	default:
		return ""
	}
}

// applyRuntimeOperationProfile selects direct actions or the opaque
// conversation menu as the sole wire owner for standard conversation
// operations. Runtime availability remains the Plugin's responsibility.
func applyRuntimeOperationProfile(snapshot map[string]any, profile pluginruntime.RuntimeOperationProfile) {
	implementations := map[string]pluginruntime.RuntimeOperationImplementation{
		"rename":  profile.Rename,
		"pin":     profile.Pin,
		"archive": profile.Archive,
		"delete":  profile.Delete,
	}
	actions := listOfMapsValue(snapshot["actions"])
	filteredActions := make([]map[string]any, 0, len(actions))
	for _, action := range actions {
		operation := semanticConversationOperation(stringFromAny(action["id"]))
		if operation == "" || implementations[operation] == pluginruntime.RuntimeOperationDirect {
			filteredActions = append(filteredActions, action)
		}
	}
	snapshot["actions"] = filteredActions

	menuOwned := false
	for _, implementation := range implementations {
		if implementation == pluginruntime.RuntimeOperationMenu {
			menuOwned = true
			break
		}
	}
	if !menuOwned {
		snapshot["conversation_menu_available"] = false
	}
}

func semanticConversationOperation(actionID string) string {
	switch strings.ToLower(strings.TrimSpace(actionID)) {
	case "rename":
		return "rename"
	case "pin":
		return "pin"
	case "archive":
		return "archive"
	case "delete":
		return "delete"
	default:
		return ""
	}
}

func runtimeProfileMetadata(service *pluginruntime.Service, pluginID string) pluginruntime.LocalMetadata {
	if service == nil {
		return pluginruntime.LocalMetadata{}
	}
	return metadataFromAdapter(service.Adapter(pluginID))
}

func fallbackCanonicalInteractiveSurface(base, live map[string]any) any {
	value, exists := live["interactive_surface"]
	if !exists {
		value = base["interactive_surface"]
	}
	item := asMap(value)
	if len(item) == 0 {
		return nil
	}
	surfaceID := strings.TrimSpace(stringFromAny(item["surface_id"]))
	kind := strings.TrimSpace(stringFromAny(item["kind"]))
	if surfaceID == "" || kind == "" {
		return nil
	}
	inputs := make([]map[string]any, 0)
	seenInputs := map[string]bool{}
	for _, input := range listOfMapsValue(item["inputs"]) {
		inputID := strings.TrimSpace(stringFromAny(input["input_id"]))
		if inputID == "" || seenInputs[inputID] {
			continue
		}
		seenInputs[inputID] = true
		inputs = append(inputs, map[string]any{
			"input_id":    inputID,
			"label":       strings.TrimSpace(stringFromAny(input["label"])),
			"placeholder": strings.TrimSpace(stringFromAny(input["placeholder"])),
			"multiline":   firstNonEmptyBool(input["multiline"]),
			"required":    firstNonEmptyBool(input["required"]),
		})
	}
	actions := make([]map[string]any, 0)
	seenActions := map[string]bool{}
	for _, action := range listOfMapsValue(item["actions"]) {
		actionID := strings.TrimSpace(stringFromAny(action["action_id"]))
		label := strings.TrimSpace(stringFromAny(action["label"]))
		if actionID == "" || label == "" || seenActions[actionID] {
			continue
		}
		seenActions[actionID] = true
		canonicalAction := map[string]any{
			"action_id":      actionID,
			"label":          label,
			"available":      !hasFalseValue(action, "available"),
			"requires_input": firstNonEmptyBool(action["requires_input"]),
			"accepts_input":  firstNonEmptyBool(action["accepts_input"]),
		}
		if kind == "questionnaire" {
			purpose, _ := action["purpose"].(string)
			if purpose != "submit_answers" && purpose != "skip_questions" {
				return nil
			}
			canonicalAction["purpose"] = purpose
		}
		actions = append(actions, canonicalAction)
	}
	if len(actions) == 0 {
		return nil
	}
	questions, validQuestions := canonicalSurfaceQuestions(kind, item["questions"])
	if !validQuestions {
		return nil
	}
	result := map[string]any{
		"surface_id":  surfaceID,
		"kind":        kind,
		"title":       strings.TrimSpace(stringFromAny(item["title"])),
		"description": strings.TrimSpace(stringFromAny(item["description"])),
		"inputs":      inputs,
		"actions":     actions,
	}
	if kind == "questionnaire" {
		result["questions"] = questions
	}
	return result
}

// composer is a live Desktop value. An explicitly supplied empty content is
// meaningful: it clears a previously mirrored draft after a native send.
func fallbackCanonicalComposer(base, live map[string]any) any {
	value, exists := live["composer"]
	if !exists {
		value = base["composer"]
	}
	if value == nil {
		return nil
	}
	composer := asMap(value)
	if len(composer) == 0 {
		return nil
	}
	return map[string]any{
		"content":  stringFromAny(composer["content"]),
		"editable": firstNonEmptyBool(composer["editable"]),
	}
}

func fallbackCanonicalQueue(base, live map[string]any) any {
	if value, ok := live["queue"]; ok {
		return canonicalQueueSnapshot(value)
	}
	return canonicalQueueSnapshot(base["queue"])
}

func fallbackCanonicalPlanMode(base, live map[string]any) any {
	value, exists := live["plan_mode"]
	if !exists {
		value = base["plan_mode"]
	}
	if value == nil {
		return nil
	}
	source := asMap(value)
	if len(source) == 0 {
		return nil
	}
	return map[string]any{
		"enabled":   firstNonEmptyBool(source["enabled"]),
		"available": firstNonEmptyBool(source["available"]),
		"actions":   canonicalPlanModeActions(source["actions"]),
	}
}

func canonicalPlanModeActions(value any) []map[string]any {
	out := make([]map[string]any, 0, 1)
	for _, item := range listOfMapsValue(value) {
		if len(out) > 0 || strings.TrimSpace(stringFromAny(item["id"])) != "plan.set" {
			continue
		}
		target := asMap(item["target"])
		enabled, ok := target["enabled"].(bool)
		if !ok {
			continue
		}
		label := strings.TrimSpace(stringFromAny(item["label"]))
		if label == "" {
			continue
		}
		out = append(out, map[string]any{
			"id":             "plan.set",
			"label":          label,
			"available":      !hasFalseValue(item, "available"),
			"requires_input": firstNonEmptyBool(item["requires_input"]),
			"target":         map[string]any{"enabled": enabled},
		})
	}
	return out
}

func fallbackCanonicalGoal(base, live map[string]any) any {
	value, exists := live["goal"]
	if !exists {
		value = base["goal"]
	}
	if value == nil {
		return nil
	}
	source := asMap(value)
	if len(source) == 0 {
		return nil
	}
	status := strings.ToLower(strings.TrimSpace(stringFromAny(source["status"])))
	if status != "running" && status != "paused" {
		status = "none"
	}
	objective := strings.TrimSpace(stringFromAny(source["objective"]))
	if len([]rune(objective)) > 8000 {
		objective = string([]rune(objective)[:8000])
	}
	if status == "none" {
		objective = ""
	}
	return map[string]any{
		"status":    status,
		"objective": objective,
		"available": firstNonEmptyBool(source["available"]),
		"actions":   canonicalGoalActions(source["actions"]),
	}
}

func canonicalGoalActions(value any) []map[string]any {
	allowed := map[string]bool{
		"goal.set": true, "goal.edit": true, "goal.pause": true,
		"goal.resume": true, "goal.clear": true,
	}
	out := make([]map[string]any, 0)
	seen := map[string]bool{}
	for _, item := range listOfMapsValue(value) {
		id := strings.ToLower(strings.TrimSpace(stringFromAny(item["id"])))
		label := strings.TrimSpace(stringFromAny(item["label"]))
		if !allowed[id] || seen[id] || label == "" {
			continue
		}
		seen[id] = true
		action := map[string]any{
			"id":             id,
			"label":          label,
			"available":      !hasFalseValue(item, "available"),
			"requires_input": firstNonEmptyBool(item["requires_input"]),
		}
		if (id == "goal.set" || id == "goal.edit") && item["target"] != nil {
			target := asMap(item["target"])
			objective := strings.TrimSpace(stringFromAny(target["objective"]))
			if objective != "" && len([]rune(objective)) <= 8000 {
				action["target"] = map[string]any{"objective": objective}
			}
		}
		out = append(out, action)
	}
	return out
}

func canonicalQueueSnapshot(value any) any {
	if value == nil {
		return nil
	}
	source := asMap(value)
	if len(source) == 0 {
		return nil
	}
	items := make([]map[string]any, 0)
	seenItems := map[string]bool{}
	for _, item := range listOfMapsValue(source["items"]) {
		id := strings.TrimSpace(stringFromAny(item["id"]))
		content := strings.TrimSpace(stringFromAny(item["content"]))
		if id == "" || content == "" || seenItems[id] {
			continue
		}
		seenItems[id] = true
		items = append(items, map[string]any{
			"id":               id,
			"content":          content,
			"actions":          canonicalQueueActions(item["actions"]),
			"has_more_actions": firstNonEmptyBool(item["has_more_actions"]),
		})
	}
	actions := canonicalQueueActions(source["actions"])
	if len(items) == 0 && len(actions) == 0 {
		return nil
	}
	return map[string]any{
		"items":   items,
		"actions": actions,
	}
}

func canonicalQueueActions(value any) []map[string]any {
	items := listOfMapsValue(value)
	out := make([]map[string]any, 0, len(items))
	seen := map[string]bool{}
	for _, item := range items {
		id := strings.TrimSpace(stringFromAny(item["id"]))
		label := strings.TrimSpace(stringFromAny(item["label"]))
		if id == "" || label == "" || seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, map[string]any{
			"id":        id,
			"label":     label,
			"available": !hasFalseValue(item, "available"),
		})
	}
	return out
}

func canonicalApprovalSnapshotValue(base, live map[string]any, run map[string]any) map[string]any {
	if value, ok := live["approval"]; ok {
		return canonicalApprovalSnapshot(value, run)
	}
	return canonicalApprovalSnapshot(base["approval"], run)
}

func detailSnapshotPayloadFromPluginEvent(event pluginruntime.PluginEvent) (map[string]any, bool) {
	if len(event.Payload) == 0 {
		return nil, false
	}
	payload := asMap(event.Payload["detail_snapshot"])
	if len(payload) == 0 {
		return nil, false
	}
	return cloneDetailMap(payload), true
}

const foregroundWatcherUnavailableEventType = "watcher.foreground.unavailable"

func foregroundWatcherUnavailableCode(event pluginruntime.PluginEvent) (string, bool) {
	if !strings.EqualFold(strings.TrimSpace(event.Type), foregroundWatcherUnavailableEventType) {
		return "", false
	}
	if !strings.EqualFold(strings.TrimSpace(stringFromAny(event.Payload["watcher"])), "foreground") {
		return "", false
	}
	code := strings.TrimSpace(stringFromAny(event.Payload["code"]))
	if code == "" {
		code = "foreground_watcher_unavailable"
	}
	return code, true
}

func canonicalRunSnapshot(source map[string]any) map[string]any {
	status := canonicalRunStatus(stringFromAny(source["status"]))
	run := map[string]any{
		"task_id":             strings.TrimSpace(stringFromAny(source["task_id"])),
		"status":              status,
		"summary":             strings.TrimSpace(stringFromAny(source["summary"])),
		"preview":             strings.TrimSpace(stringFromAny(source["preview"])),
		"interruptible":       firstNonEmptyBool(source["interruptible"]),
		"can_queue":           firstNonEmptyBool(source["can_queue"]),
		"started_at":          int64Value(source["started_at"]),
		"updated_at":          int64Value(source["updated_at"]),
		"completed_at":        int64Value(source["completed_at"]),
		"error":               strings.TrimSpace(stringFromAny(source["error"])),
		"approval_blocked":    firstNonEmptyBool(source["approval_blocked"]),
		"approval_request_id": strings.TrimSpace(stringFromAny(source["approval_request_id"])),
	}
	if phase := canonicalPhase(source["phase"]); phase != nil {
		run["phase"] = phase
	}
	if context := asMap(source["context"]); len(context) > 0 {
		run["context"] = cloneDetailMap(context)
	}
	run["trace"] = canonicalRunTrace(source)
	if run["updated_at"].(int64) == 0 {
		run["updated_at"] = time.Now().UnixMilli()
	}
	return run
}

func canonicalRunTrace(source map[string]any) map[string]any {
	raw := asMap(source["trace"])
	items := listOfMapsValue(raw["items"])
	if len(items) == 0 {
		items = listOfMapsValue(source["steps"])
	}
	trace := make([]map[string]any, 0, len(items))
	for index, item := range items {
		detail := strings.TrimSpace(firstNonEmpty(stringFromAny(item["detail"]), stringFromAny(item["text"])))
		if detail == "" {
			continue
		}
		kind := strings.ToLower(strings.TrimSpace(stringFromAny(item["kind"])))
		switch kind {
		case "thinking", "reasoning":
			kind = "reasoning"
		case "tool", "tool_result":
		default:
			kind = "status"
		}
		status := strings.ToLower(strings.TrimSpace(stringFromAny(item["status"])))
		if status != "running" && status != "completed" && status != "failed" {
			if strings.EqualFold(stringFromAny(item["kind"]), "error") {
				status = "failed"
			} else if strings.EqualFold(stringFromAny(item["kind"]), "start") || kind == "tool" {
				status = "running"
			} else {
				status = "completed"
			}
		}
		createdAt := strings.TrimSpace(firstNonEmpty(stringFromAny(item["created_at"]), stringFromAny(item["time"])))
		trace = append(trace, map[string]any{
			"id":         firstNonEmpty(stringFromAny(item["id"]), stringFromAny(item["call_id"]), fmt.Sprintf("trace:%s:%d", createdAt, index)),
			"kind":       kind,
			"title":      firstNonEmpty(stringFromAny(item["title"]), stringFromAny(item["label"]), kind),
			"detail":     detail,
			"status":     status,
			"created_at": createdAt,
		})
	}
	if len(trace) > 80 {
		trace = trace[len(trace)-80:]
	}
	return map[string]any{"items": trace, "truncated": firstNonEmptyBool(raw["truncated"]) || len(items) > 80}
}

func canonicalConversationTitle(title, conversationID string) string {
	title = strings.TrimSpace(title)
	if title == "" || title == strings.TrimSpace(conversationID) {
		return ""
	}
	lower := strings.ToLower(title)
	if strings.HasPrefix(lower, "remote:") || strings.HasPrefix(lower, "conv_") {
		return ""
	}
	if isOpaqueNativeConversationIdentity(title) {
		return ""
	}
	return title
}

// Native session keys often use a compact protocol form such as
// agent:main:session. They must never become a user-visible title merely
// because their canonical Prism conversation ID differs.
func isOpaqueNativeConversationIdentity(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" || strings.ContainsAny(value, " \t\r\n") || strings.Count(value, ":") < 2 {
		return false
	}
	for _, segment := range strings.Split(value, ":") {
		if segment == "" {
			return false
		}
		for _, r := range segment {
			if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_') {
				return false
			}
		}
	}
	return true
}

func canonicalRunStatus(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "running", "waiting":
		return "running"
	case "waiting_approval":
		return "waiting_approval"
	case "failed", "interrupted", "completed", "idle":
		return strings.ToLower(strings.TrimSpace(value))
	default:
		return "idle"
	}
}

func canonicalPrimaryAction(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "interrupt", "queue", "approval", "send":
		return strings.ToLower(strings.TrimSpace(value))
	default:
		return "send"
	}
}

func canonicalPhase(value any) map[string]any {
	phase := asMap(value)
	if len(phase) == 0 {
		return nil
	}
	return map[string]any{
		"id":     strings.TrimSpace(stringFromAny(phase["id"])),
		"label":  strings.TrimSpace(stringFromAny(phase["label"])),
		"detail": strings.TrimSpace(stringFromAny(phase["detail"])),
	}
}

func canonicalApprovalSnapshot(value any, run map[string]any) map[string]any {
	approval := asMap(value)
	if len(approval) == 0 {
		return nil
	}
	requestID := firstNonEmpty(stringFromAny(approval["approval_request_id"]), stringFromAny(approval["id"]))
	if requestID == "" {
		return nil
	}
	out := map[string]any{
		"active":              true,
		"approval_request_id": requestID,
		"task_id":             firstNonEmpty(stringFromAny(approval["task_id"]), stringFromAny(run["task_id"])),
		"title":               strings.TrimSpace(stringFromAny(approval["title"])),
		"summary":             strings.TrimSpace(stringFromAny(approval["summary"])),
		"description":         strings.TrimSpace(stringFromAny(approval["description"])),
		"created_at":          int64Value(approval["created_at"]),
		"actions":             canonicalApprovalActions(approval["actions"]),
	}
	if input := asMap(approval["input"]); len(input) > 0 {
		out["input"] = map[string]any{
			"enabled":     firstNonEmptyBool(input["enabled"]),
			"kind":        strings.TrimSpace(stringFromAny(input["kind"])),
			"label":       strings.TrimSpace(stringFromAny(input["label"])),
			"placeholder": strings.TrimSpace(stringFromAny(input["placeholder"])),
			"multiline":   firstNonEmptyBool(input["multiline"]),
		}
	}
	return out
}

func canonicalApprovalActions(value any) []map[string]any {
	items := canonicalControlOptions(value)
	for _, item := range items {
		// Action labels are presentation-only and may be localized. A plugin must
		// explicitly declare a visual style from its stable runtime action contract.
		item["style"] = firstNonEmpty(stringFromAny(item["style"]), "secondary")
		item["requires_input"] = firstNonEmptyBool(item["requires_input"])
	}
	return items
}

func canonicalControlOptions(value any) []map[string]any {
	items := listOfMapsValue(value)
	out := make([]map[string]any, 0, len(items))
	seen := map[string]bool{}
	for _, item := range items {
		canonical := canonicalControlValue(item)
		if canonical == nil {
			continue
		}
		id := stringFromAny(canonical["id"])
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		canonical["available"] = !hasFalseValue(item, "available")
		if style := strings.TrimSpace(stringFromAny(item["style"])); style != "" {
			canonical["style"] = style
		}
		if _, exists := item["requires_input"]; exists {
			canonical["requires_input"] = firstNonEmptyBool(item["requires_input"])
		}
		out = append(out, canonical)
	}
	return out
}

func canonicalControlValue(value any) map[string]any {
	item := asMap(value)
	if len(item) == 0 {
		return nil
	}
	id := firstNonEmpty(stringFromAny(item["id"]), stringFromAny(item["key"]), stringFromAny(item["value"]), stringFromAny(item["displayName"]), stringFromAny(item["display_name"]), stringFromAny(item["label"]))
	if id == "" {
		return nil
	}
	label := firstNonEmpty(stringFromAny(item["label"]), stringFromAny(item["displayName"]), stringFromAny(item["display_name"]), id)
	canonical := map[string]any{"id": id, "label": label}
	if target := canonicalControlTarget(item["target"]); len(target) > 0 {
		canonical["target"] = target
	}
	return canonical
}

// Control targets identify the exact desktop menu item observed by the plugin.
// Keep the opaque ID in the canonical detail snapshot so Mobile can pass it
// through unchanged and API can reject stale selections.
func canonicalControlTarget(value any) map[string]any {
	target := asMap(value)
	optionID := strings.TrimSpace(stringFromAny(target["option_id"]))
	if optionID == "" {
		return nil
	}
	return map[string]any{
		"option_id": optionID,
	}
}

func nullableDetailMap(value map[string]any) any {
	if len(value) == 0 {
		return nil
	}
	return value
}

func hasFalseValue(values map[string]any, key string) bool {
	value, exists := values[key]
	if !exists {
		return false
	}
	return !firstNonEmptyBool(value)
}

func cloneDetailMap(source map[string]any) map[string]any {
	out := make(map[string]any, len(source))
	for key, value := range source {
		switch typed := value.(type) {
		case map[string]any:
			out[key] = cloneDetailMap(typed)
		case []map[string]any:
			items := make([]map[string]any, 0, len(typed))
			for _, item := range typed {
				items = append(items, cloneDetailMap(item))
			}
			out[key] = items
		case []any:
			items := make([]any, 0, len(typed))
			for _, item := range typed {
				switch nested := item.(type) {
				case map[string]any:
					items = append(items, cloneDetailMap(nested))
				case []any:
					items = append(items, cloneDetailSlice(nested))
				default:
					items = append(items, nested)
				}
			}
			out[key] = items
		default:
			out[key] = value
		}
	}
	return out
}

func cloneDetailSlice(source []any) []any {
	out := make([]any, 0, len(source))
	for _, item := range source {
		switch typed := item.(type) {
		case map[string]any:
			out = append(out, cloneDetailMap(typed))
		case []any:
			out = append(out, cloneDetailSlice(typed))
		default:
			out = append(out, typed)
		}
	}
	return out
}
