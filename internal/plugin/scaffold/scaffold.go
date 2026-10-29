// Package pluginscaffold generates a runnable PluginBridge plugin skeleton from
// a template (go-stdio / node-stdio / python-stdio). The output mirrors the
// shape of the SDK examples so a freshly scaffolded plugin passes
// `prism plugin test` out of the box.
package scaffold

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Template is a supported plugin scaffold template.
type Template string

const (
	GoTemplate     Template = "go-stdio"
	NodeTemplate   Template = "node-stdio"
	PythonTemplate Template = "python-stdio"
)

// SupportedTemplates lists the templates `prism plugin init` can generate.
var SupportedTemplates = []Template{GoTemplate, NodeTemplate, PythonTemplate}

// Options controls scaffold generation.
type Options struct {
	// PluginID is the plugin's unique id, e.g. "acme.tool". Required.
	PluginID string
	// DisplayName is the human label; defaults to PluginID.
	DisplayName string
	// Template selects which language skeleton to emit.
	Template Template
	// IntegrationMode declares whether the generated scaffold targets a formal
	// native protocol or desktop automation.
	IntegrationMode string
}

// Result describes what was generated.
type Result struct {
	Dir      string   // absolute output dir
	Files    []string // absolute paths written
	Manifest string   // absolute path to pluginbridge-plugin.yaml
}

// Generate writes a plugin skeleton into dir (created if missing). It does not
// overwrite existing files except the manifest, so re-running init is safe to
// refresh the manifest while preserving hand-edited source. Returns an error if
// PluginID/Template are invalid or dir is not writable.
func Generate(dir string, opts Options) (Result, error) {
	if strings.TrimSpace(opts.PluginID) == "" {
		return Result{}, fmt.Errorf("pluginscaffold: plugin id is required")
	}
	if err := validateTemplate(opts.Template); err != nil {
		return Result{}, err
	}
	opts.PluginID = strings.ToLower(strings.TrimSpace(opts.PluginID))
	if opts.DisplayName = strings.TrimSpace(opts.DisplayName); opts.DisplayName == "" {
		opts.DisplayName = opts.PluginID
	}
	if opts.IntegrationMode = strings.TrimSpace(opts.IntegrationMode); opts.IntegrationMode == "" {
		opts.IntegrationMode = "protocol-native"
	}
	if dir == "" {
		return Result{}, fmt.Errorf("pluginscaffold: output dir is required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return Result{}, fmt.Errorf("pluginscaffold: create dir: %w", err)
	}

	abs, _ := filepath.Abs(dir)
	files, err := writeTemplate(abs, opts)
	if err != nil {
		return Result{}, err
	}
	manifest := filepath.Join(abs, "pluginbridge-plugin.yaml")
	if err := writeFileOverwrite(manifest, manifestYAML(opts)); err != nil {
		return Result{}, err
	}
	files = append(files, manifest)
	return Result{Dir: abs, Files: files, Manifest: manifest}, nil
}

func validateTemplate(t Template) error {
	for _, s := range SupportedTemplates {
		if t == s {
			return nil
		}
	}
	return fmt.Errorf("pluginscaffold: unknown template %q (supported: %s)", t, joinTemplates())
}

func joinTemplates() string {
	var ss []string
	for _, t := range SupportedTemplates {
		ss = append(ss, string(t))
	}
	return strings.Join(ss, ", ")
}

// writeFilePreserving writes content to path only if the file does not already
// exist, so init never clobbers user-edited source on a re-run. It creates
// parent directories as needed (e.g. cmd/<plugin>-adapter/).
func writeFilePreserving(path, content string) error {
	if _, err := os.Stat(path); err == nil {
		return nil // already exists, keep it
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(content), 0o644)
}

// writeFileOverwrite always writes content to path (used for the manifest,
// which is derived from options and safe to regenerate).
func writeFileOverwrite(path, content string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(content), 0o644)
}

// writeTemplate dispatches to the per-template file set, returning the absolute
// paths of all files written (manifest added separately by Generate).
func writeTemplate(dir string, opts Options) ([]string, error) {
	switch opts.Template {
	case GoTemplate:
		return writeGoTemplate(dir, opts)
	case NodeTemplate:
		return writeNodeTemplateFiles(dir, opts)
	case PythonTemplate:
		return writePythonTemplate(dir, opts)
	}
	return nil, fmt.Errorf("pluginscaffold: unhandled template %q", opts.Template)
}
