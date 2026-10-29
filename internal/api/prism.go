package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"time"

	"rokid-prism-hub/internal/config"
	"rokid-prism-hub/internal/host"
	"rokid-prism-hub/internal/realtime"
)

const (
	AccountTokenHeader     = "X-Account-Token"
	AccountInfoHeader      = "User"
	prismAPIRequestTimeout = 12 * time.Second
)

type DeviceStartResponse struct {
	DeviceCode              string `json:"device_code"`
	UserCode                string `json:"user_code"`
	VerificationURI         string `json:"verification_uri"`
	VerificationURIComplete string `json:"verification_uri_complete"`
	ExpiresIn               int64  `json:"expires_in"`
	Interval                int    `json:"interval"`
	Status                  string `json:"status"`
}

type DeviceTokenResponse struct {
	DeviceCode         string `json:"device_code"`
	UserCode           string `json:"user_code"`
	Status             string `json:"status"`
	ExpiresIn          int64  `json:"expires_in"`
	Interval           int    `json:"interval"`
	AccountID          string `json:"account_id"`
	UserID             string `json:"user_id"`
	Name               string `json:"name"`
	AccessToken        string `json:"access_token"`
	AgentInfo          string `json:"agent_info"`
	HostID             string `json:"host_id"`
	HostToken          string `json:"host_token"`
	HostAlias          string `json:"host_alias"`
	InstallID          string `json:"install_id"`
	MachineFingerprint string `json:"machine_fingerprint"`
}

type HostProfileUpdateResponse struct {
	Accepted bool                   `json:"accepted"`
	HostID   string                 `json:"host_id"`
	Profile  map[string]interface{} `json:"profile"`
}

// ResponseError preserves the API's stable error code for callers that need
// recovery behavior instead of parsing a localized error message.
type ResponseError struct {
	Status  string
	Code    string
	Message string
	Body    string
}

// IsNotFoundError reports whether an API operation already reached the
// desired absent state. DELETE callers use this to make cleanup idempotent:
// a previous successful delete must not prevent a later local cleanup retry.
func IsNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	var responseError *ResponseError
	if !errors.As(err, &responseError) || responseError == nil {
		return false
	}
	if strings.HasPrefix(strings.TrimSpace(responseError.Status), "404") {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(responseError.Code)) {
	case "not_found", "plugin_not_found", "host_plugin_not_found", "host_plugin_missing":
		return true
	default:
		return false
	}
}

func (e *ResponseError) Error() string {
	if e == nil {
		return ""
	}
	if e.Code != "" || e.Message != "" {
		return fmt.Sprintf("prism api returned %s: code=%s message=%s", e.Status, e.Code, e.Message)
	}
	return fmt.Sprintf("prism api returned %s: %s", e.Status, e.Body)
}

func StartDeviceLogin(apiURL, hostname, mode string) (DeviceStartResponse, error) {
	identity, err := host.Current()
	if err != nil {
		return DeviceStartResponse{}, err
	}
	payload := map[string]any{
		"device_name":         hostname,
		"platform":            runtimeGOOS(),
		"client_version":      realtime.ProtocolVersion,
		"install_id":          identity.InstallID,
		"machine_fingerprint": identity.MachineFingerprint,
		"mode":                mode,
	}
	var response DeviceStartResponse
	if err := DoJSON(http.MethodPost, Endpoint(apiURL, "/public/bind/start"), "", payload, &response); err != nil {
		return response, err
	}
	return response, nil
}

func PollDeviceLogin(apiURL, deviceCode string) (DeviceTokenResponse, error) {
	payload := map[string]any{"device_code": deviceCode}
	var response DeviceTokenResponse
	err := DoJSON(http.MethodPost, Endpoint(apiURL, "/public/bind/token"), "", payload, &response)
	return response, err
}

func LoginBindSession(apiURL, deviceCode, userCode, username, password, hostAlias string) (DeviceTokenResponse, error) {
	payload := map[string]any{
		"device_code": deviceCode,
		"user_code":   userCode,
		"username":    username,
		"password":    password,
		"host_alias":  hostAlias,
	}
	var response DeviceTokenResponse
	err := DoJSON(http.MethodPost, Endpoint(apiURL, "/public/bind/login"), "", payload, &response)
	return response, err
}

func DirectBindLogin(apiURL, hostname, username, password, hostAlias string) (DeviceTokenResponse, error) {
	identity, err := host.Current()
	if err != nil {
		return DeviceTokenResponse{}, err
	}
	payload := map[string]any{
		"username":            username,
		"password":            password,
		"host_alias":          hostAlias,
		"device_name":         hostname,
		"platform":            runtimeGOOS(),
		"client_version":      realtime.ProtocolVersion,
		"install_id":          identity.InstallID,
		"machine_fingerprint": identity.MachineFingerprint,
	}
	var response DeviceTokenResponse
	err = DoJSON(http.MethodPost, Endpoint(apiURL, "/public/bind/login"), "", payload, &response)
	return response, err
}

func FetchHostInfoHeader(apiURL, hostToken string) (string, error) {
	apiURL = strings.TrimRight(strings.TrimSpace(apiURL), "/")
	hostToken = strings.TrimSpace(hostToken)
	if apiURL == "" || hostToken == "" {
		return "", nil
	}
	req, err := http.NewRequest(http.MethodGet, Endpoint(apiURL, "/public/host/auth"), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Host-Token", hostToken)
	resp, err := (&http.Client{Timeout: prismAPIRequestTimeout}).Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return "", fmt.Errorf("prism host auth returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	hostInfo := strings.TrimSpace(resp.Header.Get("X-Host-Info"))
	if hostInfo == "" {
		return "", fmt.Errorf("prism host auth missing X-Host-Info")
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return hostInfo, nil
}

func UpdateUserHostProfile(apiURL string, account config.Account, hostID, alias string) (HostProfileUpdateResponse, error) {
	payload := map[string]any{
		"alias":    strings.TrimSpace(alias),
		"platform": runtimeGOOS(),
	}
	var response HostProfileUpdateResponse
	err := DoJSONWithHeaders(
		http.MethodPatch,
		Endpoint(apiURL, "/user/hosts/"+pathSegment(hostID)+"/profile"),
		userAPIHeaders(account),
		payload,
		&response,
	)
	return response, err
}

// userAPIHeaders matches the account context headers sent by the mobile client.
// Prism user routes resolve identity from User while the account token is still
// required by the gateway authentication layer.
func userAPIHeaders(account config.Account) map[string]string {
	headers := map[string]string{}
	if token := strings.TrimSpace(account.AccessToken); token != "" {
		headers[AccountTokenHeader] = token
	}
	if accountID := strings.TrimSpace(account.AccountID); accountID != "" {
		payload := struct {
			AccountID string `json:"accountId"`
			UserID    string `json:"uid,omitempty"`
			UserName  string `json:"userName,omitempty"`
		}{
			AccountID: accountID,
			UserID:    strings.TrimSpace(account.UserID),
			UserName:  strings.TrimSpace(account.Name),
		}
		encoded, err := json.Marshal(payload)
		if err == nil {
			headers[AccountInfoHeader] = url.QueryEscape(string(encoded))
		}
	}
	return headers
}

func DoJSON(method, endpoint, token string, body any, out any) error {
	headers := map[string]string{}
	if v := strings.TrimSpace(token); v != "" {
		headers[AccountTokenHeader] = v
	}
	return DoJSONWithHeaders(method, endpoint, headers, body, out)
}

func DoJSONWithHeaders(method, endpoint string, headers map[string]string, body any, out any) error {
	var bodyReader io.Reader
	if body != nil {
		buf := bytes.NewBuffer(nil)
		if err := json.NewEncoder(buf).Encode(body); err != nil {
			return err
		}
		bodyReader = buf
	}
	req, err := http.NewRequest(method, endpoint, bodyReader)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		if strings.TrimSpace(k) != "" && strings.TrimSpace(v) != "" {
			req.Header.Set(k, strings.TrimSpace(v))
		}
	}
	resp, err := (&http.Client{Timeout: prismAPIRequestTimeout}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return apiResponseError(resp.Status, responseBody)
	}
	if out == nil {
		return nil
	}
	var envelope struct {
		Success *bool           `json:"success"`
		Code    string          `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(responseBody, &envelope); err != nil {
		return err
	}
	if !isPrismAPISuccessCode(envelope.Code) || (envelope.Success != nil && !*envelope.Success) {
		return fmt.Errorf("prism api rejected request: code=%s message=%s", strings.TrimSpace(envelope.Code), strings.TrimSpace(envelope.Message))
	}
	if len(envelope.Data) == 0 || string(envelope.Data) == "null" {
		return fmt.Errorf("prism api success response missing data")
	}
	return json.Unmarshal(envelope.Data, out)
}

func isPrismAPISuccessCode(code string) bool {
	switch strings.TrimSpace(code) {
	case "0", "ok":
		return true
	default:
		return false
	}
}

func apiResponseError(status string, body []byte) error {
	var envelope struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &envelope); err == nil && (strings.TrimSpace(envelope.Code) != "" || strings.TrimSpace(envelope.Message) != "") {
		return &ResponseError{
			Status:  status,
			Code:    strings.TrimSpace(envelope.Code),
			Message: strings.TrimSpace(envelope.Message),
		}
	}
	return &ResponseError{Status: status, Body: strings.TrimSpace(string(body))}
}

func ApplyApprovedDeviceLogin(cfg *config.Config, result DeviceTokenResponse) {
	cfg.Account.AccountID = strings.TrimSpace(result.AccountID)
	cfg.Account.UserID = strings.TrimSpace(result.UserID)
	cfg.Account.Name = strings.TrimSpace(result.Name)
	cfg.Account.AccessToken = strings.TrimSpace(result.AccessToken)
	cfg.Account.AgentInfo = strings.TrimSpace(result.AgentInfo)
	cfg.Account.HostID = strings.TrimSpace(result.HostID)
	cfg.Account.HostAlias = strings.TrimSpace(result.HostAlias)
	cfg.Account.InstallID = strings.TrimSpace(result.InstallID)
	cfg.Account.MachineFingerprint = strings.TrimSpace(result.MachineFingerprint)
	// The HostID above is already replaced, so keeping the previous account's
	// host token would mix identities and make every cloud call resolve to the
	// wrong host. A login response without a token clears the old one; the
	// device pairing flow issues a fresh token for the new account.
	cfg.Prism.HostToken = strings.TrimSpace(result.HostToken)
	now := time.Now().Format(time.RFC3339)
	cfg.Account.LoginAt = now
	cfg.Account.BoundAt = now
}

func Endpoint(apiURL, path string) string {
	base := strings.TrimRight(strings.TrimSpace(apiURL), "/")
	suffix := "/" + strings.TrimLeft(strings.TrimSpace(path), "/")
	return base + suffix
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if trimmed := strings.TrimSpace(v); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func runtimeGOOS() string {
	return runtime.GOOS
}
