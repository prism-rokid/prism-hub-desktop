package bridge

import (
	"bufio"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"rokid-prism-hub/internal/host"
	"rokid-prism-hub/internal/pluginruntime"
)

// AttachmentMaterializer turns API-authorized object download URLs into the
// local, short-lived files that Plugins may inject into their native UI.
type AttachmentMaterializer interface {
	Materialize(context.Context, []string) ([]pluginruntime.Attachment, func(), error)
}

type ossAttachmentMaterializer struct {
	client *http.Client
}

const maxAttachmentBytes int64 = 100 * 1024 * 1024

// Native desktop composers retain the File.path supplied at send time and may
// consume it after the Plugin acknowledges the send. Keep successful files
// available long enough for that asynchronous read, then remove them during a
// later materialization pass.
const attachmentRetention = 24 * time.Hour

func defaultAttachmentMaterializer() AttachmentMaterializer {
	return &ossAttachmentMaterializer{
		// API already restricts the signed object source. Redirects remain disabled
		// so an approved object URL cannot change destination during materialization.
		client: &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}},
	}
}

func (m *ossAttachmentMaterializer) Materialize(ctx context.Context, input []string) ([]pluginruntime.Attachment, func(), error) {
	if len(input) == 0 {
		return nil, func() {}, nil
	}
	root := host.DefaultDataPath("attachments")
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, func() {}, fmt.Errorf("create attachment temp root: %w", err)
	}
	cleanupExpiredAttachmentDirs(root, time.Now())
	dir, err := os.MkdirTemp(root, "message-")
	if err != nil {
		return nil, func() {}, fmt.Errorf("create attachment temp directory: %w", err)
	}
	cleanup := func() { _ = os.RemoveAll(dir) }
	attachments := make([]pluginruntime.Attachment, 0, len(input))
	for index, item := range input {
		attachment, err := m.download(ctx, dir, index, item)
		if err != nil {
			cleanup()
			return nil, func() {}, err
		}
		attachments = append(attachments, attachment)
	}
	return attachments, cleanup, nil
}

func cleanupExpiredAttachmentDirs(root string, now time.Time) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "message-") {
			continue
		}
		info, infoErr := entry.Info()
		if infoErr != nil || now.Sub(info.ModTime()) < attachmentRetention {
			continue
		}
		_ = os.RemoveAll(filepath.Join(root, entry.Name()))
	}
}

func (m *ossAttachmentMaterializer) download(ctx context.Context, dir string, index int, input string) (pluginruntime.Attachment, error) {
	source := strings.TrimSpace(input)
	if source == "" {
		return pluginruntime.Attachment{}, fmt.Errorf("attachment %d has no url", index)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, source, nil)
	if err != nil {
		return pluginruntime.Attachment{}, fmt.Errorf("create attachment %d download request: %w", index, err)
	}
	resp, err := m.client.Do(req)
	if err != nil {
		return pluginruntime.Attachment{}, fmt.Errorf("download attachment %d: %w", index, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return pluginruntime.Attachment{}, fmt.Errorf("download attachment %d: http %s (%s)", index, resp.Status, attachmentDownloadDiagnostic(source, resp))
	}
	if resp.ContentLength > maxAttachmentBytes {
		return pluginruntime.Attachment{}, fmt.Errorf("download attachment %d: exceeds %d byte limit", index, maxAttachmentBytes)
	}
	name, err := attachmentFilename(resp, source)
	if err != nil {
		return pluginruntime.Attachment{}, fmt.Errorf("attachment %d: %w", index, err)
	}
	body := bufio.NewReader(resp.Body)
	mimeType := attachmentMIMEType(resp.Header.Get("Content-Type"), name, body)
	// Names are part of the Plugin-facing descriptor, but need not be unique on
	// disk. Prefix the local copy so one message may legitimately contain files
	// with the same original name.
	localPath := filepath.Join(dir, fmt.Sprintf("%03d-%s", index, name))
	file, err := os.OpenFile(localPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return pluginruntime.Attachment{}, fmt.Errorf("create attachment %d local file: %w", index, err)
	}
	// Content-Length is optional and cannot be trusted. Read one extra byte so
	// chunked responses receive the same 100 MiB limit without retaining their
	// complete payload in memory.
	written, copyErr := io.Copy(file, io.LimitReader(body, maxAttachmentBytes+1))
	closeErr := file.Close()
	if written > maxAttachmentBytes {
		_ = os.Remove(localPath)
		return pluginruntime.Attachment{}, fmt.Errorf("download attachment %d: exceeds %d byte limit", index, maxAttachmentBytes)
	}
	if copyErr != nil {
		_ = os.Remove(localPath)
		return pluginruntime.Attachment{}, fmt.Errorf("write attachment %d: %w", index, copyErr)
	}
	if closeErr != nil {
		_ = os.Remove(localPath)
		return pluginruntime.Attachment{}, fmt.Errorf("close attachment %d: %w", index, closeErr)
	}
	return pluginruntime.Attachment{
		Name:      name,
		MIMEType:  mimeType,
		LocalPath: localPath,
	}, nil
}

// attachmentDownloadDiagnostic keeps failed signed downloads actionable without
// placing their query string (and therefore temporary credentials) in logs.
func attachmentDownloadDiagnostic(rawURL string, response *http.Response) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "source=unparseable"
	}
	fields := []string{"host=" + parsed.Host, "path=" + parsed.EscapedPath()}
	if response != nil {
		for _, header := range []string{"X-Oss-Request-Id", "X-Request-Id", "X-Cdn-Request-Id"} {
			if requestID := strings.TrimSpace(response.Header.Get(header)); requestID != "" {
				fields = append(fields, "request_id="+requestID)
				break
			}
		}
		// OSS error responses contain a stable machine code such as
		// SignatureDoesNotMatch. Keep it for diagnosis without logging the
		// signed URL, object key, token, or response message.
		var ossError struct {
			Code string `xml:"Code"`
		}
		if response.Body != nil {
			body, readErr := io.ReadAll(io.LimitReader(response.Body, 8*1024))
			if readErr != nil {
				return strings.Join(fields, " ")
			}
			_ = xml.Unmarshal(body, &ossError)
			if code := strings.TrimSpace(ossError.Code); code != "" {
				fields = append(fields, "oss_code="+code)
			}
		}
	}
	return strings.Join(fields, " ")
}

// attachmentMIMEType makes a downloaded object usable by native desktop
// composers even when object storage omits or generalizes Content-Type. The
// response body remains buffered so MIME sniffing never consumes file bytes.
func attachmentMIMEType(header, name string, body *bufio.Reader) string {
	if mediaType, _, err := mime.ParseMediaType(strings.TrimSpace(header)); err == nil && !genericAttachmentMIMEType(mediaType) {
		return strings.ToLower(mediaType)
	}
	if extensionType := strings.TrimSpace(mime.TypeByExtension(strings.ToLower(filepath.Ext(name)))); extensionType != "" {
		if mediaType, _, err := mime.ParseMediaType(extensionType); err == nil && !genericAttachmentMIMEType(mediaType) {
			return strings.ToLower(mediaType)
		}
	}
	if body != nil {
		if sample, err := body.Peek(512); err == nil || len(sample) > 0 {
			if detected := strings.TrimSpace(http.DetectContentType(sample)); !genericAttachmentMIMEType(detected) {
				return strings.ToLower(detected)
			}
		}
	}
	if mediaType, _, err := mime.ParseMediaType(strings.TrimSpace(header)); err == nil && mediaType != "" {
		return strings.ToLower(mediaType)
	}
	return "application/octet-stream"
}

func genericAttachmentMIMEType(value string) bool {
	mediaType, _, err := mime.ParseMediaType(strings.TrimSpace(value))
	if err != nil {
		return true
	}
	switch strings.ToLower(mediaType) {
	case "", "application/octet-stream", "application/binary", "binary/octet-stream":
		return true
	default:
		return false
	}
}

func attachmentFilename(resp *http.Response, rawURL string) (string, error) {
	if disposition := strings.TrimSpace(resp.Header.Get("Content-Disposition")); disposition != "" {
		_, params, err := mime.ParseMediaType(disposition)
		if err == nil && strings.TrimSpace(params["filename"]) != "" {
			return safeAttachmentFilename(params["filename"])
		}
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("parse attachment url: %w", err)
	}
	name, err := url.PathUnescape(path.Base(parsed.Path))
	if err != nil {
		return "", fmt.Errorf("decode attachment filename: %w", err)
	}
	return safeAttachmentFilename(name)
}

func safeAttachmentFilename(value string) (string, error) {
	name := filepath.Base(strings.TrimSpace(value))
	if name == "" || name == "." || name == string(filepath.Separator) {
		return "", fmt.Errorf("attachment filename missing from download URL")
	}
	return name, nil
}
