package bridge

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"rokid-prism-hub/internal/host"
)

func TestAttachmentMaterializerDownloadsAndCleansLocalFile(t *testing.T) {
	t.Setenv(host.PrismDataDirEnv, t.TempDir())
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "image/png")
		writer.Header().Set("Content-Disposition", `attachment; filename="screen.png"`)
		_, _ = writer.Write([]byte("image-bytes"))
	}))
	defer server.Close()

	materializer := defaultAttachmentMaterializer()
	attachments, cleanup, err := materializer.Materialize(context.Background(), []string{server.URL + "/opaque-object"})
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	if len(attachments) != 1 {
		t.Fatalf("expected one attachment, got %+v", attachments)
	}
	attachment := attachments[0]
	if attachment.Name != "screen.png" || attachment.MIMEType != "image/png" || filepath.Base(attachment.LocalPath) != "000-screen.png" {
		t.Fatalf("unexpected local attachment: %+v", attachment)
	}
	bytes, err := os.ReadFile(attachment.LocalPath)
	if err != nil || string(bytes) != "image-bytes" {
		t.Fatalf("unexpected materialized file bytes=%q err=%v", bytes, err)
	}
	cleanup()
	if _, err := os.Stat(filepath.Dir(attachment.LocalPath)); !os.IsNotExist(err) {
		t.Fatalf("expected materialized directory cleanup, err=%v", err)
	}
}

func TestAttachmentMaterializerRemovesExpiredMessageDirectories(t *testing.T) {
	root := t.TempDir()
	expired := filepath.Join(root, "message-expired")
	fresh := filepath.Join(root, "message-fresh")
	if err := os.MkdirAll(expired, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(fresh, 0o700); err != nil {
		t.Fatal(err)
	}
	then := time.Now()
	if err := os.Chtimes(expired, then.Add(-attachmentRetention-time.Minute), then.Add(-attachmentRetention-time.Minute)); err != nil {
		t.Fatal(err)
	}
	cleanupExpiredAttachmentDirs(root, then)
	if _, err := os.Stat(expired); !os.IsNotExist(err) {
		t.Fatalf("expected expired attachment directory removal, err=%v", err)
	}
	if _, err := os.Stat(fresh); err != nil {
		t.Fatalf("expected fresh attachment directory retained, err=%v", err)
	}
}

func TestAttachmentMaterializerPreservesSignedQuery(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if got, want := request.URL.Query().Get("Expires"), "1786000000"; got != want {
			t.Fatalf("Expires query=%q, want %q", got, want)
		}
		if got, want := request.URL.Query().Get("Signature"), "a+b/c="; got != want {
			t.Fatalf("Signature query=%q, want %q", got, want)
		}
		if got, want := request.URL.Query().Get("security-token"), "temporary token"; got != want {
			t.Fatalf("security-token query=%q, want %q", got, want)
		}
		writer.Header().Set("Content-Disposition", `attachment; filename="signed.txt"`)
		_, _ = writer.Write([]byte("signed attachment"))
	}))
	defer server.Close()

	attachments, cleanup, err := defaultAttachmentMaterializer().Materialize(context.Background(), []string{
		server.URL + "/dev/prism/tmp/file/request/signed.txt?OSSAccessKeyId=STS.test&Expires=1786000000&Signature=a%2Bb%2Fc%3D&security-token=temporary%20token",
	})
	defer cleanup()
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	if len(attachments) != 1 || attachments[0].Name != "signed.txt" {
		t.Fatalf("unexpected attachments: %+v", attachments)
	}
}

func TestAttachmentDownloadDiagnosticRedactsQuery(t *testing.T) {
	response := &http.Response{Header: http.Header{"X-Oss-Request-Id": []string{"oss-request-123"}}}
	diagnostic := attachmentDownloadDiagnostic("https://cdn.example.com/dev/prism/tmp/file/request/secret.txt?Signature=very-secret", response)
	if !strings.Contains(diagnostic, "host=cdn.example.com") || !strings.Contains(diagnostic, "path=/dev/prism/tmp/file/request/secret.txt") || !strings.Contains(diagnostic, "request_id=oss-request-123") {
		t.Fatalf("unexpected diagnostic: %s", diagnostic)
	}
	if strings.Contains(diagnostic, "Signature") || strings.Contains(diagnostic, "very-secret") {
		t.Fatalf("query leaked in diagnostic: %s", diagnostic)
	}
}

func TestAttachmentDownloadDiagnosticIncludesOssCodeWithoutResponseDetails(t *testing.T) {
	response := &http.Response{
		Header: http.Header{"X-Oss-Request-Id": []string{"oss-request-456"}},
		Body:   io.NopCloser(strings.NewReader(`<?xml version="1.0"?><Error><Code>SignatureDoesNotMatch</Code><Message>secret object detail</Message></Error>`)),
	}
	diagnostic := attachmentDownloadDiagnostic("https://bucket.oss-cn-hangzhou.aliyuncs.com/dev/prism/tmp/file/request/secret.txt?Signature=very-secret", response)
	if !strings.Contains(diagnostic, "oss_code=SignatureDoesNotMatch") {
		t.Fatalf("missing OSS error code: %s", diagnostic)
	}
	if strings.Contains(diagnostic, "secret object detail") || strings.Contains(diagnostic, "very-secret") {
		t.Fatalf("sensitive diagnostic detail leaked: %s", diagnostic)
	}
}

func TestAttachmentMaterializerRejectsOversizedChunkedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "text/plain")
		writer.Header().Set("Content-Disposition", `attachment; filename="large.txt"`)
		writer.Header().Set("Transfer-Encoding", "chunked")
		_, _ = io.CopyN(writer, zeroReader{}, maxAttachmentBytes+1)
	}))
	defer server.Close()

	_, cleanup, err := defaultAttachmentMaterializer().Materialize(context.Background(), []string{server.URL + "/large.txt"})
	defer cleanup()
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("Materialize error = %v, want size limit", err)
	}
}

type zeroReader struct{}

func (zeroReader) Read(value []byte) (int, error) {
	for index := range value {
		value[index] = 0
	}
	return len(value), nil
}

func TestAttachmentMaterializerRejectsRedirect(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = writer.Write([]byte("should not be fetched"))
	}))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, target.URL+"/file.txt", http.StatusFound)
	}))
	defer redirect.Close()

	_, cleanup, err := defaultAttachmentMaterializer().Materialize(context.Background(), []string{redirect.URL + "/file.txt"})
	defer cleanup()
	if err == nil {
		t.Fatal("expected redirect download to fail")
	}
}

func TestAttachmentMaterializerKeepsDuplicateDisplayNames(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Disposition", `attachment; filename="report.txt"`)
		_, _ = writer.Write([]byte(request.URL.Path))
	}))
	defer server.Close()

	attachments, cleanup, err := defaultAttachmentMaterializer().Materialize(context.Background(), []string{
		server.URL + "/first",
		server.URL + "/second",
	})
	defer cleanup()
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	if len(attachments) != 2 || attachments[0].Name != "report.txt" || attachments[1].Name != "report.txt" {
		t.Fatalf("expected duplicate display names, got %+v", attachments)
	}
	if attachments[0].LocalPath == attachments[1].LocalPath {
		t.Fatalf("expected distinct local paths, got %q", attachments[0].LocalPath)
	}
}

func TestAttachmentMaterializerInfersImageMIMETypeWhenOSSUsesGenericType(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/octet-stream")
		writer.Header().Set("Content-Disposition", `attachment; filename="mobile-image.png"`)
		_, _ = writer.Write([]byte("opaque image object"))
	}))
	defer server.Close()

	attachments, cleanup, err := defaultAttachmentMaterializer().Materialize(context.Background(), []string{server.URL + "/opaque-object"})
	defer cleanup()
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	if len(attachments) != 1 || attachments[0].MIMEType != "image/png" {
		t.Fatalf("expected image/png inferred from attachment filename, got %+v", attachments)
	}
}

func TestAttachmentMaterializerSniffsImageWhenFilenameHasNoExtension(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/octet-stream")
		writer.Header().Set("Content-Disposition", `attachment; filename="opaque"`)
		_, _ = writer.Write([]byte("\x89PNG\r\n\x1a\n"))
	}))
	defer server.Close()

	attachments, cleanup, err := defaultAttachmentMaterializer().Materialize(context.Background(), []string{server.URL + "/opaque-object"})
	defer cleanup()
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	if len(attachments) != 1 || attachments[0].MIMEType != "image/png" {
		t.Fatalf("expected image/png sniffed from attachment bytes, got %+v", attachments)
	}
}
