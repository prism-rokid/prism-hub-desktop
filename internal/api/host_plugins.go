package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"rokid-prism-hub/internal/config"
)

type HostPlugin struct {
	PluginID        string         `json:"plugin_id"`
	DisplayName     string         `json:"display_name"`
	HostID          string         `json:"host_id"`
	HostAlias       string         `json:"host_alias"`
	DeviceName      string         `json:"device_name"`
	Address         string         `json:"address"`
	HostAddress     string         `json:"host_address"`
	Description     string         `json:"description"`
	IconURL         string         `json:"icon_url"`
	Version         string         `json:"version"`
	IntegrationMode string         `json:"integration_mode"`
	Capabilities    map[string]any `json:"capabilities"`
	CreatedAt       int64          `json:"created_at"`
	UpdatedAt       int64          `json:"updated_at"`
}

type HostPluginList struct {
	HostID      string       `json:"host_id"`
	HostAlias   string       `json:"host_alias"`
	DeviceName  string       `json:"device_name"`
	Address     string       `json:"address"`
	HostAddress string       `json:"host_address"`
	Items       []HostPlugin `json:"items"`
}

func ListUserHostPlugins(apiURL string, account config.Account, hostID string) (HostPluginList, error) {
	var response HostPluginList
	err := DoJSONWithHeaders(http.MethodGet, Endpoint(apiURL, "/user/hosts/"+pathSegment(hostID)+"/plugins"), userAPIHeaders(account), nil, &response)
	return response, err
}

func RegisterUserHostPlugin(apiURL string, account config.Account, hostID string, payload map[string]any) (map[string]any, error) {
	var response map[string]any
	err := DoJSONWithHeaders(http.MethodPost, Endpoint(apiURL, "/user/hosts/"+pathSegment(hostID)+"/plugins"), userAPIHeaders(account), payload, &response)
	return response, err
}

// UpdateUserHostPluginMetadata changes the user-owned presentation metadata of an
// already registered Plugin. Registration must not overwrite these values.
func UpdateUserHostPluginMetadata(apiURL string, account config.Account, hostID, pluginID string, payload map[string]any) (map[string]any, error) {
	var response map[string]any
	err := DoJSONWithHeaders(http.MethodPut, Endpoint(apiURL, "/user/hosts/"+pathSegment(hostID)+"/plugins/"+pathSegment(pluginID)), userAPIHeaders(account), payload, &response)
	return response, err
}

func DeleteUserHostPlugin(apiURL string, account config.Account, hostID, pluginID string) (map[string]any, error) {
	var response map[string]any
	err := DoJSONWithHeaders(http.MethodDelete, Endpoint(apiURL, "/user/hosts/"+pathSegment(hostID)+"/plugins/"+pathSegment(pluginID)), userAPIHeaders(account), nil, &response)
	return response, err
}

// MarkUserConversationRead advances the account-local read watermark after the
// native desktop watcher confirms that a conversation is actually foreground.
func MarkUserConversationRead(ctx context.Context, apiURL string, account config.Account, hostID, pluginID, conversationID string) error {
	endpoint := Endpoint(apiURL, "/user/hosts/"+pathSegment(hostID)+"/plugins/"+pathSegment(pluginID)+"/conversations/"+pathSegment(conversationID)+":read")
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	for key, value := range userAPIHeaders(account) {
		req.Header.Set(key, value)
	}
	resp, err := (&http.Client{Timeout: prismAPIRequestTimeout}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return apiResponseError(resp.Status, body)
	}
	var envelope struct {
		Success *bool  `json:"success"`
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return err
	}
	if !isPrismAPISuccessCode(envelope.Code) || (envelope.Success != nil && !*envelope.Success) {
		return fmt.Errorf("prism api rejected request: code=%s message=%s", strings.TrimSpace(envelope.Code), strings.TrimSpace(envelope.Message))
	}
	return nil
}

func pathSegment(value string) string {
	return url.PathEscape(strings.TrimSpace(value))
}
