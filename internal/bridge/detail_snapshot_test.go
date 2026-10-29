package bridge

import (
	"testing"

	"rokid-prism-hub/internal/pluginruntime"
)

func TestCanonicalDetailSnapshotUsesOnlyCanonicalFields(t *testing.T) {
	snapshot := canonicalDetailSnapshot(
		map[string]any{
			"conversation_id": "conv-1",
			"plugin_id":       "codex",
			"title":           "Canonical detail",
			"cwd":             "/workspace/demo",
		},
		map[string]any{
			"current_model":          map[string]any{"id": "gpt-5.6", "label": "GPT-5.6"},
			"current_reasoning_mode": map[string]any{"id": "high", "label": "High"},
			"current_approval_mode":  map[string]any{"id": "never", "label": "Full Access"},
			"approval_options":       []map[string]any{{"id": "never", "label": "Full Access"}},
			"current_run":            map[string]any{"status": "running"},
			"run": map[string]any{
				"task_id":      "task-1",
				"status":       "completed",
				"completed_at": int64(1234),
				"phase": map[string]any{
					"id":     "inspect",
					"label":  "Inspecting",
					"detail": "documentation",
				},
			},
			"approval": map[string]any{
				"approval_request_id": "approval-1",
				"actions": []map[string]any{
					{"id": "deny", "label": "Deny"},
					{"id": "approve", "label": "Approve"},
				},
			},
		},
		"messages-1",
	)

	for _, legacy := range []string{
		"controls",
		"metadata",
		"current_run",
		"current_runtime",
		"current_reasoning_mode",
		"current_approval_mode",
		"approval_options",
	} {
		if _, exists := snapshot[legacy]; exists {
			t.Fatalf("canonical detail contains legacy field %q: %#v", legacy, snapshot[legacy])
		}
	}
	if snapshot["current_reasoning"] != nil || snapshot["current_permission"] != nil {
		t.Fatalf("legacy current aliases must not populate canonical values: %#v", snapshot)
	}
	run := asMap(snapshot["run"])
	if run["completed_at"] != int64(1234) {
		t.Fatalf("unexpected completed_at: %#v", run["completed_at"])
	}
	phase := asMap(run["phase"])
	if phase["id"] != "inspect" || phase["detail"] != "documentation" {
		t.Fatalf("unexpected run phase: %#v", phase)
	}
	approval := asMap(snapshot["approval"])
	actions := listOfMapsValue(approval["actions"])
	if len(actions) != 2 || actions[0]["id"] != "deny" || actions[1]["id"] != "approve" {
		t.Fatalf("approval action order changed: %#v", actions)
	}
}

func TestCanonicalTerminalDetailAndCacheFailClosed(t *testing.T) {
	base := map[string]any{"plugin_id": "claudecode", "conversation_id": "conv-1"}
	live := map[string]any{"terminal": map[string]any{
		"status": "detached", "input_owner": "panel", "can_send": true,
		"can_interrupt": true, "can_approve": false,
	}}
	snapshot := canonicalDetailSnapshot(base, live, "")
	terminal := asMap(snapshot["terminal"])
	if terminal["status"] != "detached" || terminal["input_owner"] != "panel" || terminal["can_send"] != true {
		t.Fatalf("terminal state was not preserved: %#v", terminal)
	}
	if _, ok := snapshot["desktop_foreground"]; ok {
		t.Fatalf("native terminal must not imply desktop foreground: %#v", snapshot)
	}

	missing := canonicalDetailSnapshot(snapshot, map[string]any{"run": map[string]any{"status": "running"}}, "")
	if got := asMap(missing["terminal"]); got["can_send"] != false || got["reason"] != "terminal_state_unobserved" {
		t.Fatalf("partial live detail reused cached terminal control: %#v", got)
	}

	store := NewDetailSnapshotStore()
	store.PutForRuntime("claudecode", snapshot)
	changed := store.MarkRuntimeUnavailable("claudecode")
	if len(changed) != 1 {
		t.Fatalf("expected one revoked terminal detail, got %d", len(changed))
	}
	stale, ok := store.GetForRuntime("claudecode", "conv-1")
	if !ok || asMap(stale["terminal"])["can_send"] != false || asMap(stale["terminal"])["status"] != "reconnecting" {
		t.Fatalf("runtime loss did not revoke cached terminal controls: %#v", stale)
	}
	store.PutForRuntime("claudecode", snapshot)
	recovered, _ := store.GetForRuntime("claudecode", "conv-1")
	if asMap(recovered["terminal"])["can_send"] != true {
		t.Fatalf("fresh watcher detail did not restore controls: %#v", recovered)
	}
}

func TestCanonicalDetailSnapshotNormalizesTraceAndSuppressesInternalTitle(t *testing.T) {
	snapshot := canonicalDetailSnapshot(
		map[string]any{"conversation_id": "conv_019f", "title": "conv_019f"},
		map[string]any{"run": map[string]any{"steps": []any{
			map[string]any{"kind": "thinking", "label": "思考", "text": "读取配置", "time": "2026-07-27T10:00:00Z"},
			map[string]any{"kind": "tool", "label": "工具", "text": "Read project files", "call_id": "call-1"},
		}}},
		"",
	)
	if title := snapshot["title"]; title != "" {
		t.Fatalf("internal conversation id must not be a display title: %#v", title)
	}
	trace := asMap(asMap(snapshot["run"])["trace"])
	items := listOfMapsValue(trace["items"])
	if len(items) != 2 || items[0]["kind"] != "reasoning" || items[1]["id"] != "call-1" {
		t.Fatalf("unexpected canonical trace: %#v", trace)
	}
}

func TestCanonicalConversationTitleSuppressesOpaqueNativeSessionKey(t *testing.T) {
	if got := canonicalConversationTitle("agent:main:main", "prism-conversation-1"); got != "" {
		t.Fatalf("native session key must not be visible title: %q", got)
	}
	if got := canonicalConversationTitle("A real conversation title", "prism-conversation-1"); got != "A real conversation title" {
		t.Fatalf("real title was unexpectedly suppressed: %q", got)
	}
}

func TestDetailSnapshotStoreMarksForegroundWatcherUnavailable(t *testing.T) {
	store := NewDetailSnapshotStore()
	store.PutForRuntime("codex", map[string]any{
		"plugin_id":          "codex",
		"conversation_id":    "conv-1",
		"desktop_foreground": true,
		"detail_stale":       false,
		"current_model":      map[string]any{"id": "gpt-5.7"},
	})
	store.MarkForegroundWatcherUnavailable("codex")
	if !store.IsForegroundWatcherUnavailable("codex") {
		t.Fatal("expected foreground watcher to be unavailable")
	}
	if store.IsForegroundForRuntime("codex", "conv-1") {
		t.Fatal("unavailable watcher must not retain foreground truth")
	}
	stale, ok := store.GetForRuntime("codex", "conv-1")
	if !ok || firstNonEmptyBool(stale["desktop_foreground"]) || !firstNonEmptyBool(stale["detail_stale"]) {
		t.Fatalf("expected cached detail to remain explicitly stale, got %#v", stale)
	}

	store.PutForRuntime("codex", map[string]any{
		"plugin_id":          "codex",
		"conversation_id":    "conv-1",
		"desktop_foreground": true,
		"detail_stale":       false,
		"current_model":      map[string]any{"id": "gpt-5.7"},
	})
	if store.IsForegroundWatcherUnavailable("codex") {
		t.Fatal("valid foreground detail must recover watcher availability")
	}
}

func TestDetailSnapshotStoreReturnsDeepCopy(t *testing.T) {
	store := NewDetailSnapshotStore()
	store.PutForRuntime("codex", map[string]any{
		"conversation_id": "conv-1",
		"plugin_id":       "codex",
		"run":             map[string]any{"status": "running"},
		"actions":         []map[string]any{{"id": "pin", "label": "Pin"}},
	})

	first, ok := store.GetForRuntime("codex", "conv-1")
	if !ok {
		t.Fatal("expected stored detail snapshot")
	}
	asMap(first["run"])["status"] = "failed"
	listOfMapsValue(first["actions"])[0]["label"] = "Changed"

	second, ok := store.GetForRuntime("codex", "conv-1")
	if !ok {
		t.Fatal("expected stored detail snapshot")
	}
	if asMap(second["run"])["status"] != "running" {
		t.Fatalf("nested run was mutated through Get: %#v", second["run"])
	}
	if listOfMapsValue(second["actions"])[0]["label"] != "Pin" {
		t.Fatalf("nested actions were mutated through Get: %#v", second["actions"])
	}
}

func TestDetailSnapshotStoreTracksCurrentForegroundConversation(t *testing.T) {
	store := NewDetailSnapshotStore()
	store.PutForRuntime("codex", map[string]any{
		"conversation_id":    "conv-1",
		"plugin_id":          "codex",
		"desktop_foreground": true,
		"detail_stale":       false,
	})
	store.PutForRuntime("codex", map[string]any{
		"conversation_id": "conv-2",
		"plugin_id":       "codex",
	})
	store.PutForRuntime("codex", map[string]any{
		"conversation_id":    "conv-3",
		"plugin_id":          "codex",
		"desktop_foreground": true,
		"detail_stale":       false,
	})

	first, ok := store.GetForRuntime("codex", "conv-1")
	if !ok {
		t.Fatal("expected first stored snapshot")
	}
	if firstNonEmptyBool(first["desktop_foreground"]) || !firstNonEmptyBool(first["detail_stale"]) {
		t.Fatalf("expected previous foreground snapshot to become stale, got %#v", first)
	}

	latest, ok := store.GetForRuntime("codex", "conv-3")
	if !ok {
		t.Fatal("expected latest stored snapshot")
	}
	if !firstNonEmptyBool(latest["desktop_foreground"]) || firstNonEmptyBool(latest["detail_stale"]) {
		t.Fatalf("expected latest foreground snapshot to stay fresh, got %#v", latest)
	}
}

func TestCanonicalDetailSnapshotPreservesExistingOptionsOnPartialLiveUpdate(t *testing.T) {
	snapshot := canonicalDetailSnapshot(
		map[string]any{
			"conversation_id": "conv-1",
			"plugin_id":       "codex",
			"current_model":   map[string]any{"id": "gpt-5.4", "label": "GPT-5.4"},
			"model_options":   []map[string]any{{"id": "gpt-5.4", "label": "GPT-5.4"}, {"id": "gpt-5.6", "label": "GPT-5.6"}},
			"approval": map[string]any{
				"approval_request_id": "approval-1",
				"actions":             []map[string]any{{"id": "approve", "label": "批准"}},
			},
			"run": map[string]any{
				"status":         "running",
				"primary_action": "queue",
				"can_queue":      true,
			},
		},
		map[string]any{
			"current_model": map[string]any{"id": "gpt-5.6", "label": "GPT-5.6"},
			"run": map[string]any{
				"status":         "running",
				"primary_action": "queue",
				"can_queue":      true,
			},
		},
		"",
	)

	if current := asMap(snapshot["current_model"]); current["id"] != "gpt-5.6" {
		t.Fatalf("expected live current model override, got %#v", current)
	}
	options := listOfMapsValue(snapshot["model_options"])
	if len(options) != 2 {
		t.Fatalf("expected base options preserved on partial live update, got %#v", options)
	}
	approval := asMap(snapshot["approval"])
	if approval["approval_request_id"] != "approval-1" {
		t.Fatalf("expected base approval preserved when live payload omits approval, got %#v", approval)
	}
}

func TestCanonicalDetailSnapshotCarriesAndClearsDesktopComposer(t *testing.T) {
	base := map[string]any{
		"conversation_id": "conv-1",
		"plugin_id":       "codex",
		"composer": map[string]any{
			"content":  "queued draft",
			"editable": true,
		},
	}

	updated := canonicalDetailSnapshot(base, map[string]any{
		"composer": map[string]any{
			"content":  "edited queued draft",
			"editable": true,
		},
	}, "")
	composer := asMap(updated["composer"])
	if composer["content"] != "edited queued draft" || !firstNonEmptyBool(composer["editable"]) {
		t.Fatalf("expected live desktop composer, got %#v", composer)
	}

	cleared := canonicalDetailSnapshot(base, map[string]any{
		"composer": map[string]any{
			"content":  "",
			"editable": false,
		},
	}, "")
	composer = asMap(cleared["composer"])
	if composer["content"] != "" || firstNonEmptyBool(composer["editable"]) {
		t.Fatalf("expected explicit empty composer to clear cached draft, got %#v", composer)
	}
}

func TestCanonicalDetailSnapshotAcceptsGenericJSONArrayOptions(t *testing.T) {
	snapshot := canonicalDetailSnapshot(
		map[string]any{
			"conversation_id": "conv-1",
			"plugin_id":       "codex",
		},
		map[string]any{
			"current_model": map[string]any{"id": "gpt-5.6", "label": "GPT-5.6"},
			"model_options": []any{
				map[string]any{
					"id": "gpt-5.6", "label": "GPT-5.6", "available": true,
					"target": map[string]any{"option_id": "gpt-5.6"},
				},
				map[string]any{"id": "gpt-5.4", "label": "GPT-5.4", "available": true},
			},
			"actions": []any{
				map[string]any{"id": "model.switch", "label": "切换模型", "available": true},
				map[string]any{"id": "reasoning.switch", "label": "切换推理", "available": true},
			},
			"permission_options": []any{
				map[string]any{"key": "never", "label": "完全访问权限", "value": "never", "available": true},
			},
		},
		"messages-1",
	)

	if current := asMap(snapshot["current_model"]); current["id"] != "gpt-5.6" {
		t.Fatalf("expected current model preserved, got %#v", current)
	}
	if options := listOfMapsValue(snapshot["model_options"]); len(options) != 2 {
		t.Fatalf("expected generic JSON model options preserved, got %#v", snapshot["model_options"])
	} else if target := asMap(options[0]["target"]); target["option_id"] != "gpt-5.6" || len(target) != 1 {
		t.Fatalf("expected structured control target preserved, got %#v", options[0])
	}
	if actions := listOfMapsValue(snapshot["actions"]); len(actions) != 2 {
		t.Fatalf("expected generic JSON actions preserved, got %#v", snapshot["actions"])
	}
	if permission := listOfMapsValue(snapshot["permission_options"]); len(permission) != 1 {
		t.Fatalf("expected generic JSON permission options preserved, got %#v", snapshot["permission_options"])
	}
}

func TestCanonicalDetailSnapshotPreservesDynamicInteractiveControls(t *testing.T) {
	snapshot := canonicalDetailSnapshot(
		map[string]any{
			"conversation_id": "conv-1",
			"plugin_id":       "codex",
		},
		map[string]any{
			"current_model": nil,
			"model_options": []any{},
			"interactive_controls": []any{map[string]any{
				"control_id":    "codex.intelligence:abc",
				"display_label": "Model",
				"current_value": "5.5",
				"options": []any{map[string]any{
					"option_id": "codex.intelligence:abc:one",
					"label":     "5.5",
					"available": true,
				}},
			}},
		},
		"",
	)
	controls := listOfMapsValue(snapshot["interactive_controls"])
	if len(controls) != 1 || controls[0]["control_id"] != "codex.intelligence:abc" {
		t.Fatalf("expected dynamic controls preserved, got %#v", snapshot["interactive_controls"])
	}
	if current := snapshot["current_model"]; current != nil {
		t.Fatalf("expected explicit dynamic null to clear a legacy current model, got %#v", current)
	}
}

func TestRuntimeControlProfileKeepsOnlyInteractiveOwner(t *testing.T) {
	snapshot := canonicalDetailSnapshot(nil, map[string]any{
		"conversation_id":   "conv-1",
		"plugin_id":         "demo",
		"current_model":     map[string]any{"id": "model-a", "label": "Model A"},
		"current_reasoning": map[string]any{"id": "high", "label": "High"},
		"model_options":     []any{map[string]any{"id": "model-a", "label": "Model A"}},
		"reasoning_options": []any{map[string]any{"id": "high", "label": "High"}},
		"interactive_controls": []any{
			map[string]any{"control_id": "demo.intelligence", "display_label": "Model and reasoning", "current_value": "Model A / High", "semantic_kinds": []any{"model", "reasoning"}},
			map[string]any{"control_id": "demo.custom", "display_label": "Custom", "current_value": "On"},
		},
		"actions": []any{
			map[string]any{"id": "model.switch", "label": "Switch model"},
			map[string]any{"id": "compact", "label": "Compact"},
		},
	}, "")
	snapshot = applyRuntimeProfile(snapshot, pluginruntime.LocalMetadata{
		CurrentRuntimeMode: "desktop", RuntimeControlsDeclared: true,
		RuntimeControls: pluginruntime.RuntimeControlProfile{
			Model: pluginruntime.RuntimeControlInteractive, Reasoning: pluginruntime.RuntimeControlInteractive,
		},
	})
	if snapshot["runtime_mode"] != "desktop" || snapshot["current_model"] != nil || snapshot["current_reasoning"] != nil {
		t.Fatalf("interactive runtime retained stable current values: %#v", snapshot)
	}
	if len(listOfMapsValue(snapshot["model_options"])) != 0 || len(listOfMapsValue(snapshot["reasoning_options"])) != 0 {
		t.Fatalf("interactive runtime retained stable options: %#v", snapshot)
	}
	if controls := listOfMapsValue(snapshot["interactive_controls"]); len(controls) != 2 {
		t.Fatalf("interactive and custom controls must remain: %#v", controls)
	}
	if actions := listOfMapsValue(snapshot["actions"]); len(actions) != 1 || actions[0]["id"] != "compact" {
		t.Fatalf("interactive runtime retained stable control action: %#v", actions)
	}
}

func TestRuntimeControlProfileKeepsOnlyStableOwner(t *testing.T) {
	snapshot := canonicalDetailSnapshot(nil, map[string]any{
		"conversation_id": "conv-1",
		"plugin_id":       "demo",
		"current_model":   map[string]any{"id": "model-a", "label": "Model A"},
		"model_options":   []any{map[string]any{"id": "model-a", "label": "Model A"}},
		"interactive_controls": []any{
			map[string]any{"control_id": "demo.model", "display_label": "Model", "current_value": "Model A", "semantic_kinds": []any{"model"}},
		},
	}, "")
	snapshot = applyRuntimeProfile(snapshot, pluginruntime.LocalMetadata{
		CurrentRuntimeMode: "native", RuntimeControlsDeclared: true,
		RuntimeControls: pluginruntime.RuntimeControlProfile{Model: pluginruntime.RuntimeControlStable},
	})
	if asMap(snapshot["current_model"])["id"] != "model-a" || len(listOfMapsValue(snapshot["model_options"])) != 1 {
		t.Fatalf("stable runtime lost stable control: %#v", snapshot)
	}
	if controls := listOfMapsValue(snapshot["interactive_controls"]); len(controls) != 0 {
		t.Fatalf("stable runtime retained duplicate interactive owner: %#v", controls)
	}
}

func TestRuntimeOperationProfileKeepsOnlyDirectOwners(t *testing.T) {
	snapshot := canonicalDetailSnapshot(nil, map[string]any{
		"conversation_id":             "conv-ops",
		"plugin_id":                   "demo",
		"conversation_menu_available": true,
		"actions": []any{
			map[string]any{"id": "message.send", "available": true},
			map[string]any{"id": "rename", "available": true},
			map[string]any{"id": "pin", "available": true},
			map[string]any{"id": "archive", "available": true},
			map[string]any{"id": "delete", "available": true},
		},
	}, "")
	snapshot = applyRuntimeProfile(snapshot, pluginruntime.LocalMetadata{
		CurrentRuntimeMode: "native", RuntimeOperationsDeclared: true,
		RuntimeOperations: pluginruntime.RuntimeOperationProfile{
			Rename:  pluginruntime.RuntimeOperationDirect,
			Pin:     pluginruntime.RuntimeOperationDirect,
			Archive: pluginruntime.RuntimeOperationUnsupported,
			Delete:  pluginruntime.RuntimeOperationUnsupported,
		},
	})
	if snapshot["runtime_mode"] != "native" || firstNonEmptyBool(snapshot["conversation_menu_available"]) {
		t.Fatalf("direct-only runtime retained a conversation menu: %#v", snapshot)
	}
	actions := listOfMapsValue(snapshot["actions"])
	if len(actions) != 3 || actions[0]["id"] != "message.send" || actions[1]["id"] != "rename" || actions[2]["id"] != "pin" {
		t.Fatalf("runtime operation owner filtering failed: %#v", actions)
	}
}

func TestRuntimeOperationProfileKeepsOnlyMenuOwner(t *testing.T) {
	snapshot := canonicalDetailSnapshot(nil, map[string]any{
		"conversation_id":             "conv-menu",
		"plugin_id":                   "demo",
		"conversation_menu_available": true,
		"actions": []any{
			map[string]any{"id": "rename", "available": true},
			map[string]any{"id": "pin", "available": true},
		},
	}, "")
	snapshot = applyRuntimeProfile(snapshot, pluginruntime.LocalMetadata{
		CurrentRuntimeMode: "desktop", RuntimeOperationsDeclared: true,
		RuntimeOperations: pluginruntime.RuntimeOperationProfile{
			Rename: pluginruntime.RuntimeOperationMenu,
			Pin:    pluginruntime.RuntimeOperationMenu,
		},
	})
	if !firstNonEmptyBool(snapshot["conversation_menu_available"]) {
		t.Fatalf("menu runtime lost its Plugin-confirmed menu trigger: %#v", snapshot)
	}
	if actions := listOfMapsValue(snapshot["actions"]); len(actions) != 0 {
		t.Fatalf("menu runtime retained duplicate direct operations: %#v", actions)
	}
}

func TestRuntimeOperationProfileDoesNotInventMenuAvailability(t *testing.T) {
	snapshot := canonicalDetailSnapshot(nil, map[string]any{
		"conversation_id": "conv-menu-closed",
		"plugin_id":       "demo",
	}, "")
	snapshot = applyRuntimeProfile(snapshot, pluginruntime.LocalMetadata{
		CurrentRuntimeMode: "desktop", RuntimeOperationsDeclared: true,
		RuntimeOperations: pluginruntime.RuntimeOperationProfile{Rename: pluginruntime.RuntimeOperationMenu},
	})
	if firstNonEmptyBool(snapshot["conversation_menu_available"]) {
		t.Fatalf("manifest declaration invented runtime menu availability: %#v", snapshot)
	}
}

func TestDetailSnapshotStoreResetPluginIsIsolated(t *testing.T) {
	store := NewDetailSnapshotStore()
	store.PutForRuntime("one", map[string]any{"conversation_id": "conv-1", "desktop_foreground": true})
	store.PutForRuntime("two", map[string]any{"conversation_id": "conv-2", "desktop_foreground": true})
	store.ResetPlugin("one")
	if _, ok := store.GetForRuntime("one", "conv-1"); ok || store.ForegroundConversationID("one") != "" {
		t.Fatal("reset retained previous runtime detail")
	}
	if _, ok := store.GetForRuntime("two", "conv-2"); !ok {
		t.Fatal("reset removed another plugin runtime detail")
	}
}

func TestCanonicalDetailSnapshotPreservesClosedInteractiveControl(t *testing.T) {
	snapshot := canonicalDetailSnapshot(
		map[string]any{"conversation_id": "conv-1", "plugin_id": "codex"},
		map[string]any{"interactive_controls": []any{map[string]any{
			"control_id":     "codex.permission",
			"display_label":  "permissions",
			"current_value":  "Ask me",
			"menu_available": true,
		}}},
		"",
	)
	controls := listOfMapsValue(snapshot["interactive_controls"])
	if len(controls) != 1 {
		t.Fatalf("expected closed interactive control preserved, got %#v", snapshot["interactive_controls"])
	}
	if controls[0]["menu_available"] != true {
		t.Fatalf("expected menu availability preserved, got %#v", controls[0])
	}
	if _, exists := controls[0]["options"]; exists {
		t.Fatalf("closed interactive control must not synthesize options, got %#v", controls[0])
	}
}

func TestCanonicalDetailSnapshotPreservesAndClearsQueue(t *testing.T) {
	base := map[string]any{
		"conversation_id": "conv-1",
		"plugin_id":       "codex",
		"queue": map[string]any{
			"items": []any{map[string]any{
				"id":               "native-message-1",
				"content":          "queued message",
				"has_more_actions": true,
				"actions": []any{map[string]any{
					"id": "queue.item.guide", "label": "Guide", "available": true,
				}},
			}},
			"actions": []any{map[string]any{
				"id": "queue.resume", "label": "Continue", "available": true,
			}},
		},
	}
	preserved := canonicalDetailSnapshot(base, map[string]any{}, "")
	queue := asMap(preserved["queue"])
	items := listOfMapsValue(queue["items"])
	if len(items) != 1 || items[0]["id"] != "native-message-1" || items[0]["content"] != "queued message" {
		t.Fatalf("expected queue item preserved, got %#v", preserved["queue"])
	}
	if actions := listOfMapsValue(items[0]["actions"]); len(actions) != 1 || actions[0]["id"] != "queue.item.guide" {
		t.Fatalf("expected item actions preserved, got %#v", items[0]["actions"])
	}
	if items[0]["has_more_actions"] != true {
		t.Fatalf("expected native queue overflow availability preserved, got %#v", items[0])
	}
	if actions := listOfMapsValue(queue["actions"]); len(actions) != 1 || actions[0]["id"] != "queue.resume" {
		t.Fatalf("expected queue actions preserved, got %#v", queue["actions"])
	}

	cleared := canonicalDetailSnapshot(base, map[string]any{"queue": nil}, "")
	if cleared["queue"] != nil {
		t.Fatalf("expected explicit live queue null to clear cached queue, got %#v", cleared["queue"])
	}
}

func TestCanonicalDetailSnapshotPreservesContextWindowFields(t *testing.T) {
	snapshot := canonicalDetailSnapshot(
		map[string]any{
			"conversation_id":              "conv-1",
			"plugin_id":                    "codex",
			"context_window_total":         "258400",
			"context_tokens_used":          "120000",
			"context_window_usage_percent": "46",
			"context_window":               "上下文窗口 258400",
		},
		map[string]any{},
		"messages-1",
	)

	if snapshot["context_window_total"] != "258400" {
		t.Fatalf("expected context_window_total preserved, got %#v", snapshot["context_window_total"])
	}
	if snapshot["context_tokens_used"] != "120000" {
		t.Fatalf("expected context_tokens_used preserved, got %#v", snapshot["context_tokens_used"])
	}
	if snapshot["context_window_usage_percent"] != "46" {
		t.Fatalf("expected context_window_usage_percent preserved, got %#v", snapshot["context_window_usage_percent"])
	}
	if snapshot["context_window"] != "上下文窗口 258400" {
		t.Fatalf("expected context_window preserved, got %#v", snapshot["context_window"])
	}
}

func TestCanonicalDetailSnapshotClearsApprovalWhenLiveExplicitlyClearsIt(t *testing.T) {
	snapshot := canonicalDetailSnapshot(
		map[string]any{
			"conversation_id": "conv-1",
			"plugin_id":       "codex",
			"approval": map[string]any{
				"approval_request_id": "approval-1",
				"actions":             []map[string]any{{"id": "approve", "label": "批准"}},
			},
			"run": map[string]any{
				"status":              "waiting_approval",
				"approval_request_id": "approval-1",
				"primary_action":      "approval",
			},
		},
		map[string]any{
			"approval": nil,
			"run": map[string]any{
				"status":         "idle",
				"primary_action": "send",
			},
		},
		"",
	)

	if snapshot["approval"] != nil {
		t.Fatalf("expected live approval clear to remove approval snapshot, got %#v", snapshot["approval"])
	}
	run := asMap(snapshot["run"])
	if run["status"] != "idle" || run["primary_action"] != "send" {
		t.Fatalf("expected run to clear approval state, got %#v", run)
	}
}

func TestDetailSnapshotPayloadFromPluginEvent(t *testing.T) {
	detail, ok := detailSnapshotPayloadFromPluginEvent(pluginruntime.PluginEvent{
		Type: "desktop.state.changed",
		Payload: map[string]any{
			"detail_snapshot": map[string]any{
				"primary_action": "queue",
				"run": map[string]any{
					"status": "running",
				},
			},
		},
	})
	if !ok {
		t.Fatal("expected detail snapshot payload")
	}
	if stringFromAny(detail["primary_action"]) != "queue" {
		t.Fatalf("unexpected detail payload: %#v", detail)
	}
}

func TestDetailSnapshotStoreForegroundSnapshots(t *testing.T) {
	store := NewDetailSnapshotStore()
	store.PutForRuntime("codex", map[string]any{
		"conversation_id":    "conv-1",
		"plugin_id":          "codex",
		"title":              "Foreground",
		"desktop_foreground": true,
		"detail_stale":       false,
		"actions":            []map[string]any{{"id": "model.switch", "label": "切换模型"}},
	})
	store.PutForRuntime("codex", map[string]any{
		"conversation_id": "conv-2",
		"plugin_id":       "codex",
		"title":           "Stale",
	})

	items := store.ForegroundSnapshots()
	if len(items) != 1 {
		t.Fatalf("expected exactly one foreground snapshot, got %#v", items)
	}
	if stringFromAny(items[0]["conversation_id"]) != "conv-1" {
		t.Fatalf("unexpected foreground snapshot: %#v", items[0])
	}
	if !firstNonEmptyBool(items[0]["desktop_foreground"]) || firstNonEmptyBool(items[0]["detail_stale"]) {
		t.Fatalf("expected foreground snapshot to stay fresh, got %#v", items[0])
	}
}

func TestCanonicalApprovalSnapshotDropsNonCanonicalFields(t *testing.T) {
	snapshot := canonicalDetailSnapshot(
		map[string]any{
			"conversation_id": "conv-1",
			"plugin_id":       "codex",
		},
		map[string]any{
			"approval": map[string]any{
				"id":                  "approval-live-1",
				"approval_request_id": "approval-live-1",
				"title":               "等待审批",
				"summary":             "等待审批",
				"description":         "rm -rf build",
				"status":              "waiting_approval",
				"source":              "desktop_live",
				"method":              "desktop.live.dialog",
				"command":             "rm -rf build",
				"source_device":       "desktop",
				"allow_permanent":     true,
				"actions":             []map[string]any{{"id": "approve", "label": "批准", "style": "primary", "requires_input": false}},
			},
			"run": map[string]any{
				"status":         "waiting_approval",
				"primary_action": "approval",
			},
		},
		"",
	)

	approval := asMap(snapshot["approval"])
	if approval["approval_request_id"] != "approval-live-1" || approval["title"] != "等待审批" {
		t.Fatalf("expected canonical approval fields preserved, got %#v", approval)
	}
	for _, field := range []string{"id", "status", "source", "method", "command", "source_device", "allow_permanent"} {
		if _, exists := approval[field]; exists {
			t.Fatalf("expected non-canonical approval field %q dropped, got %#v", field, approval)
		}
	}
}

func TestCanonicalApprovalActionsUseOnlyExplicitStyle(t *testing.T) {
	actions := canonicalApprovalActions([]map[string]any{
		{"id": "approve", "label": "批准", "style": "primary"},
		{"id": "deny", "label": "Reject", "style": "danger"},
		{"id": "note", "label": "批准并附言", "style": "primary", "requires_input": true},
		{"id": "other", "label": "Reject without explicit style"},
	})
	if len(actions) != 4 {
		t.Fatalf("unexpected action count: %#v", actions)
	}
	if actions[0]["style"] != "primary" {
		t.Fatalf("expected explicit primary style preserved, got %#v", actions[0])
	}
	if actions[1]["style"] != "danger" {
		t.Fatalf("expected explicit danger style preserved, got %#v", actions[1])
	}
	if actions[2]["style"] != "primary" || actions[2]["requires_input"] != true {
		t.Fatalf("expected explicit note action style/input preserved, got %#v", actions[2])
	}
	if actions[3]["style"] != "secondary" {
		t.Fatalf("expected localized/untyped action to remain secondary, got %#v", actions[3])
	}
}

func TestCanonicalDetailSnapshotPreservesOnlyDeclaredInteractiveSurfaceFields(t *testing.T) {
	snapshot := canonicalDetailSnapshot(map[string]any{
		"conversation_id": "conv-1",
		"plugin_id":       "codex",
	}, map[string]any{
		"interactive_surface": map[string]any{
			"surface_id":  "surface-1",
			"kind":        "dialog",
			"title":       "Confirm",
			"description": "Discard the queued message before sending the new message?",
			"unknown":     "must-not-leak",
			"inputs": []map[string]any{{
				"input_id": "input-1", "label": "Reason", "placeholder": "optional", "required": true,
			}},
			"actions": []map[string]any{{
				"action_id": "cancel", "label": "Cancel", "available": true, "requires_input": false, "accepts_input": true, "dom_index": 1,
			}},
		},
	}, "")

	surface := asMap(snapshot["interactive_surface"])
	if surface["surface_id"] != "surface-1" || surface["kind"] != "dialog" {
		t.Fatalf("expected canonical surface identity, got %#v", surface)
	}
	if surface["description"] != "Discard the queued message before sending the new message?" {
		t.Fatalf("expected desktop surface description preserved, got %#v", surface)
	}
	if _, exists := surface["unknown"]; exists {
		t.Fatalf("unexpected non-canonical surface field: %#v", surface)
	}
	action := listOfMapsValue(surface["actions"])[0]
	if _, exists := action["dom_index"]; exists {
		t.Fatalf("unexpected internal action field: %#v", action)
	}
	if !firstNonEmptyBool(action["accepts_input"]) {
		t.Fatalf("expected accepts_input to remain in canonical surface: %#v", action)
	}
}

func TestCanonicalDetailSnapshotMergesPlanModeAndGoal(t *testing.T) {
	base := map[string]any{
		"conversation_id": "conv-1",
		"plugin_id":       "codex",
		"plan_mode": map[string]any{
			"enabled": false, "available": true,
			"actions": []map[string]any{{
				"id": "plan.set", "label": "开启计划模式", "available": true,
				"target": map[string]any{"enabled": true},
			}},
		},
		"goal": map[string]any{
			"status": "running", "objective": "Ship the release", "available": true,
			"actions": []map[string]any{{"id": "goal.pause", "label": "暂停目标"}},
		},
	}
	preserved := canonicalDetailSnapshot(base, map[string]any{}, "")
	plan := asMap(preserved["plan_mode"])
	if firstNonEmptyBool(plan["enabled"]) || !firstNonEmptyBool(plan["available"]) {
		t.Fatalf("expected inherited plan state, got %#v", plan)
	}
	planAction := listOfMapsValue(plan["actions"])[0]
	if !firstNonEmptyBool(asMap(planAction["target"])["enabled"]) {
		t.Fatalf("expected canonical plan target, got %#v", planAction)
	}
	goal := asMap(preserved["goal"])
	if goal["status"] != "running" || goal["objective"] != "Ship the release" {
		t.Fatalf("expected inherited goal state, got %#v", goal)
	}

	updated := canonicalDetailSnapshot(base, map[string]any{
		"plan_mode": map[string]any{
			"enabled": true, "available": true,
			"actions": []map[string]any{{
				"id": "plan.set", "label": "关闭计划模式", "target": map[string]any{"enabled": false},
			}},
		},
		"goal": map[string]any{
			"status": "paused", "objective": "Ship the release", "available": true,
			"actions": []map[string]any{{"id": "goal.resume", "label": "恢复目标"}},
		},
	}, "")
	if !firstNonEmptyBool(asMap(updated["plan_mode"])["enabled"]) || asMap(updated["goal"])["status"] != "paused" {
		t.Fatalf("expected live plan and goal to replace base, got %#v", updated)
	}
}

func TestCanonicalDetailSnapshotClearsAndFiltersPlanModeAndGoal(t *testing.T) {
	base := map[string]any{
		"conversation_id": "conv-1",
		"plugin_id":       "codex",
		"plan_mode":       map[string]any{"enabled": true, "available": true},
		"goal":            map[string]any{"status": "running", "objective": "Old goal", "available": true},
	}
	cleared := canonicalDetailSnapshot(base, map[string]any{"plan_mode": nil, "goal": nil}, "")
	if cleared["plan_mode"] != nil || cleared["goal"] != nil {
		t.Fatalf("expected explicit live null to clear plan and goal, got %#v", cleared)
	}

	filtered := canonicalDetailSnapshot(nil, map[string]any{
		"conversation_id": "conv-1",
		"plugin_id":       "codex",
		"plan_mode": map[string]any{
			"enabled": true, "available": true, "internal": "drop",
			"actions": []map[string]any{
				{"id": "plan.set", "label": "invalid target", "target": map[string]any{"enabled": "false"}},
				{"id": "unknown", "label": "drop", "target": map[string]any{"enabled": false}},
			},
		},
		"goal": map[string]any{
			"status": "invalid", "objective": "must clear for none", "available": true, "internal": "drop",
			"actions": []map[string]any{
				{"id": "goal.set", "label": "设置目标", "available": false, "requires_input": true, "unknown": "drop"},
				{"id": "goal.unknown", "label": "drop"},
			},
		},
	}, "")
	plan := asMap(filtered["plan_mode"])
	if len(listOfMapsValue(plan["actions"])) != 0 {
		t.Fatalf("expected invalid plan targets to be rejected, got %#v", plan)
	}
	if _, exists := plan["internal"]; exists {
		t.Fatalf("unexpected plan internal field: %#v", plan)
	}
	goal := asMap(filtered["goal"])
	if goal["status"] != "none" || goal["objective"] != "" {
		t.Fatalf("expected invalid goal status to normalize to none, got %#v", goal)
	}
	goalActions := listOfMapsValue(goal["actions"])
	if len(goalActions) != 1 || goalActions[0]["id"] != "goal.set" || !hasFalseValue(goalActions[0], "available") {
		t.Fatalf("expected only canonical unavailable goal action, got %#v", goalActions)
	}
	if _, exists := goalActions[0]["unknown"]; exists {
		t.Fatalf("unexpected goal action internal field: %#v", goalActions[0])
	}
}

func TestDetailSnapshotStoreClearsInteractiveSurfaceWhenConversationBecomesStale(t *testing.T) {
	store := NewDetailSnapshotStore()
	store.PutForRuntime("codex", map[string]any{
		"conversation_id":    "conv-1",
		"plugin_id":          "codex",
		"desktop_foreground": true,
		"detail_stale":       false,
		"interactive_surface": map[string]any{
			"surface_id": "surface-1", "kind": "dialog", "actions": []map[string]any{{"action_id": "cancel", "label": "Cancel"}},
		},
	})
	store.PutForRuntime("codex", map[string]any{
		"conversation_id": "conv-2", "plugin_id": "codex", "desktop_foreground": true, "detail_stale": false,
	})

	stale, ok := store.GetForRuntime("codex", "conv-1")
	if !ok || !firstNonEmptyBool(stale["detail_stale"]) || stale["interactive_surface"] != nil {
		t.Fatalf("expected stale detail without interaction surface, got %#v", stale)
	}
}
