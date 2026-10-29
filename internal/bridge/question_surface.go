package bridge

// Preserve the question schema explicitly; never forward native request
// metadata, callback identities or DOM implementation details to clients.
func canonicalSurfaceQuestions(kind string, value any) ([]map[string]any, bool) {
	if kind != "questionnaire" {
		return nil, true
	}
	rows := listOfMapsValue(value)
	if len(rows) == 0 || len(rows) > 20 {
		return nil, false
	}
	result := make([]map[string]any, 0, len(rows))
	ids := map[string]bool{}
	for _, row := range rows {
		id, idOK := row["id"].(string)
		question, questionOK := row["question"].(string)
		if !idOK || id == "" || ids[id] || !questionOK || question == "" || len(question) > 32768 {
			return nil, false
		}
		ids[id] = true
		options := make([]map[string]any, 0)
		seen := map[string]bool{}
		for _, option := range listOfMapsValue(row["options"]) {
			optionID, okID := option["id"].(string)
			label, okLabel := option["label"].(string)
			if !okID || !okLabel || optionID == "" || label == "" || seen[optionID] || len(options) >= 100 {
				return nil, false
			}
			seen[optionID] = true
			options = append(options, map[string]any{"id": optionID, "label": label})
		}
		custom := firstNonEmptyBool(row["allow_custom"])
		if len(options) == 0 && !custom {
			return nil, false
		}
		placeholder, _ := row["placeholder"].(string)
		result = append(result, map[string]any{"id": id, "question": question, "multiple": firstNonEmptyBool(row["multiple"]), "allow_custom": custom, "placeholder": placeholder, "options": options})
	}
	return result, true
}
