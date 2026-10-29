package plugin

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"unicode"
)

// PluginBridge v4 has one wire naming rule: snake_case. Runtime structs keep
// Go names; conversion happens only at the stdio boundary.
func marshalWire(value any) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var decoded any
	if err := json.Unmarshal(data, &decoded); err != nil {
		return nil, err
	}
	return json.Marshal(toSnakeWire(decoded))
}

func unmarshalWire(data []byte, out any) error {
	var decoded any
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if err := validateSnakeWire(decoded); err != nil {
		return err
	}
	canonical, err := json.Marshal(toPascalWireForType(decoded, reflect.TypeOf(out)))
	if err != nil {
		return err
	}
	return json.Unmarshal(canonical, out)
}

func validateSnakeWire(value any) error {
	switch typed := value.(type) {
	case map[string]any:
		for key, item := range typed {
			if key != snakeWireKey(key) {
				return fmt.Errorf("pluginbridge v4 requires snake_case wire field %q", key)
			}
			if err := validateSnakeWire(item); err != nil {
				return err
			}
		}
	case []any:
		for _, item := range typed {
			if err := validateSnakeWire(item); err != nil {
				return err
			}
		}
	}
	return nil
}

func toSnakeWire(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[snakeWireKey(key)] = toSnakeWire(item)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for index, item := range typed {
			out[index] = toSnakeWire(item)
		}
		return out
	default:
		// ProcessAdapter passes runtime structs as request params. Convert those
		// through JSON before applying the wire naming rule.
		if value != nil {
			kind := reflect.TypeOf(value).Kind()
			if kind == reflect.Struct || kind == reflect.Pointer || kind == reflect.Slice || kind == reflect.Array || kind == reflect.Map {
				data, err := json.Marshal(value)
				if err == nil {
					var decoded any
					if json.Unmarshal(data, &decoded) == nil {
						return toSnakeWire(decoded)
					}
				}
			}
		}
		return value
	}
}

// toPascalWireForType converts only fields owned by a typed protocol struct.
// Dynamic map/interface values are already valid snake_case payloads and must
// remain byte-for-byte key compatible with Plugin authors' contracts.
func toPascalWireForType(value any, target reflect.Type) any {
	for target != nil && target.Kind() == reflect.Pointer {
		target = target.Elem()
	}
	if target == nil {
		return value
	}

	switch target.Kind() {
	case reflect.Struct:
		typed, ok := value.(map[string]any)
		if !ok {
			return value
		}
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			field, outputKey, found := wireStructField(target, key)
			if !found {
				// json.Unmarshal ignores unknown struct fields. Keep the wire key
				// unchanged rather than mutating an extension field's contract.
				out[key] = item
				continue
			}
			out[outputKey] = toPascalWireForType(item, field.Type)
		}
		return out
	case reflect.Map:
		typed, ok := value.(map[string]any)
		if !ok || target.Key().Kind() != reflect.String {
			return value
		}
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[key] = toPascalWireForType(item, target.Elem())
		}
		return out
	case reflect.Slice, reflect.Array:
		typed, ok := value.([]any)
		if !ok {
			return value
		}
		out := make([]any, len(typed))
		for index, item := range typed {
			out[index] = toPascalWireForType(item, target.Elem())
		}
		return out
	case reflect.Interface:
		return value
	default:
		return value
	}
}

func wireStructField(target reflect.Type, wireKey string) (reflect.StructField, string, bool) {
	for index := 0; index < target.NumField(); index++ {
		field := target.Field(index)
		if field.PkgPath != "" || field.Tag.Get("json") == "-" {
			continue
		}
		tagName := strings.Split(field.Tag.Get("json"), ",")[0]
		outputKey := tagName
		if outputKey == "" {
			outputKey = field.Name
		}
		if wireKey == snakeWireKey(outputKey) || wireKey == snakeWireKey(field.Name) {
			return field, outputKey, true
		}
	}
	return reflect.StructField{}, "", false
}

func snakeWireKey(key string) string {
	var out strings.Builder
	runes := []rune(key)
	for index, char := range runes {
		if unicode.IsUpper(char) && index > 0 && (unicode.IsLower(runes[index-1]) || (index+1 < len(runes) && unicode.IsLower(runes[index+1]))) {
			out.WriteByte('_')
		}
		out.WriteRune(unicode.ToLower(char))
	}
	return out.String()
}

func pascalWireKey(key string) string {
	parts := strings.Split(key, "_")
	for index, part := range parts {
		switch part {
		case "id":
			parts[index] = "ID"
		case "url":
			parts[index] = "URL"
		case "uri":
			parts[index] = "URI"
		case "pid":
			parts[index] = "PID"
		case "ms":
			parts[index] = "MS"
		case "api":
			parts[index] = "API"
		case "cwd":
			parts[index] = "Cwd"
		default:
			parts[index] = strings.ToUpper(part[:1]) + part[1:]
		}
	}
	return strings.Join(parts, "")
}
