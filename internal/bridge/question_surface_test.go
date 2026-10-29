package bridge

import "testing"

func TestQuestionnaireSurvivesDetailCanonicalization(t *testing.T) {
	surface := map[string]any{"surface_id": "question:test", "kind": "questionnaire", "title": "问题", "questions": []any{map[string]any{"id": "q1", "question": "今晚想吃什么？", "allow_custom": true, "options": []any{map[string]any{"id": "0", "label": "火锅"}}, "native_metadata": "private"}}, "actions": []any{map[string]any{"action_id": "submit", "label": "发送", "purpose": "submit_answers", "accepts_input": true, "requires_input": true}}}
	result := asMap(fallbackCanonicalInteractiveSurface(nil, map[string]any{"interactive_surface": surface}))
	questions := listOfMapsValue(result["questions"])
	if len(questions) != 1 || questions[0]["question"] != "今晚想吃什么？" {
		t.Fatalf("questions dropped: %#v", result)
	}
	if _, exists := questions[0]["native_metadata"]; exists {
		t.Fatal("native metadata escaped")
	}
	actions := listOfMapsValue(result["actions"])
	if len(actions) != 1 || actions[0]["purpose"] != "submit_answers" {
		t.Fatalf("action purpose dropped: %#v", result)
	}
	surface["actions"] = []any{map[string]any{"action_id": "bad", "label": "bad", "purpose": "unknown"}}
	if fallbackCanonicalInteractiveSurface(nil, map[string]any{"interactive_surface": surface}) != nil {
		t.Fatal("accepted unknown question action")
	}
}
