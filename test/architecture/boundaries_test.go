// boundaries_test.go exercises static inward dependency and owning-schema boundaries in the
// offline test and diagnostic infrastructure.

// Package architecture_test provides static inward dependency and owning-schema boundaries in the
// offline test and diagnostic infrastructure.
package architecture_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// module pins the repository import prefix for static Hexagonal Architecture boundary checks.
const module = "github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public"

// Deliberately explicit: adding a core dependency requires reviewing its purity.
// This also blocks infrastructure hidden behind an otherwise innocent helper.
var pureImports = map[string]bool{
	"context": true, "errors": true, "fmt": true, "math": true,
	"math/big": true, "math/rand/v2": true, "regexp": true,
	"slices": true, "strings": true, "time": true,
}

// within checks path membership on directory boundaries so similarly prefixed sibling packages do
// not match.
func within(path, root string) bool { return path == root || strings.HasPrefix(path, root+"/") }

// importViolation classifies a candidate import against the owning service/layer dependency rules.
func importViolation(service, layer, imp string) string {
	own := module + "/services/" + service
	if strings.HasPrefix(imp, module+"/services/") && !within(imp, own) {
		return "cross-service dependency"
	}
	if layer != "domain" && layer != "application" {
		return ""
	}
	if layer == "domain" && within(imp, own+"/internal/application") {
		return "domain depends on application"
	}
	if pureImports[imp] || within(imp, own+"/internal/domain") ||
		layer == "application" && within(imp, own+"/internal/application") {
		return ""
	}
	return "unapproved core dependency (infrastructure or outside the inward boundary)"
}

// TestHexagonalArchitecture inspects repository imports and schema references to enforce inward
// service boundaries.
func TestHexagonalArchitecture(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate repository")
	}
	root := filepath.Join(filepath.Dir(file), "../..")
	for _, service := range []string{"extractor-service", "transformer-service", "loader-service"} {
		t.Run(service, func(t *testing.T) {
			dir := filepath.Join(root, "services", service)
			groups := map[string][]*ast.File{}
			set := token.NewFileSet()
			err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				// Include build-tagged/platform-specific production files; exclude test harnesses.
				if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
					return nil
				}
				f, err := parser.ParseFile(set, path, nil, 0)
				if err != nil {
					return err
				}
				rel, err := filepath.Rel(dir, path)
				if err != nil {
					return err
				}
				layer := ""
				parts := strings.Split(filepath.ToSlash(rel), "/")
				if len(parts) > 2 && parts[0] == "internal" {
					layer = parts[1]
				}
				for _, spec := range f.Imports {
					imp, err := strconv.Unquote(spec.Path.Value)
					if err != nil {
						return err
					}
					if reason := importViolation(service, layer, imp); reason != "" {
						t.Errorf("%s: %s: %s", set.Position(spec.Pos()), reason, imp)
					}
				}
				groups[filepath.Dir(path)] = append(groups[filepath.Dir(path)], f)
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(groups) == 0 {
				t.Fatal("no production packages inspected")
			}
			owner := strings.TrimSuffix(service, "-service")
			for _, files := range groups {
				for _, violation := range goSchemaViolations(set, files, owner) {
					t.Error(violation)
				}
			}
			migrations, err := filepath.Glob(filepath.Join(root, "migrations", owner, "*.sql"))
			if err != nil || len(migrations) == 0 {
				t.Fatal("missing service migrations", err)
			}
			for _, path := range migrations {
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				for _, foreign := range foreignSchemas(string(data), owner) {
					t.Errorf("%s: foreign schema %s", path, foreign)
				}
			}
		})
	}
}

// Guard schema-qualified references, including quoted identifiers and regclass
// strings. This is a static tripwire, not a SQL parser or a privilege boundary.
// Runtime-built SQL and search_path indirection still need permission tests.
var schemaReference = regexp.MustCompile(`(?i)\b(extractor|transformer|loader)"?\s*\.\s*"?[a-z_]`)

// sqlComments removes SQL comments before policy matching so comment text is not mistaken for
// executed schema access.
var sqlComments = regexp.MustCompile(`(?s)/\*.*?\*/|--[^\n]*`)

// foreignSchemas identifies forbidden owning-schema references after excluding SQL comments from
// policy input.
func foreignSchemas(text, owner string) []string {
	text = sqlComments.ReplaceAllString(text, " ")
	var result []string
	for _, match := range schemaReference.FindAllStringSubmatch(text, -1) {
		schema := strings.ToLower(match[1])
		if schema != owner {
			result = append(result, schema)
		}
	}
	return result
}

// goSchemaViolations inspects Go syntax for static SQL/schema use that crosses the service
// ownership boundary.
func goSchemaViolations(set *token.FileSet, files []*ast.File, owner string) []string {
	// Fold literal concatenations and package constants, including across files.
	constants := map[string]ast.Expr{}
	for _, f := range files {
		for _, decl := range f.Decls {
			d, ok := decl.(*ast.GenDecl)
			if !ok || d.Tok != token.CONST {
				continue
			}
			for _, spec := range d.Specs {
				v := spec.(*ast.ValueSpec)
				for i, name := range v.Names {
					if i < len(v.Values) {
						constants[name.Name] = v.Values[i]
					}
				}
			}
		}
	}
	var violations []string
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			// Import paths and Go comments are not SQL.
			if _, ok := n.(*ast.ImportSpec); ok {
				return false
			}
			expr, ok := n.(ast.Expr)
			if !ok {
				return true
			}
			text, ok := constantString(expr, constants, map[string]bool{})
			if !ok {
				return true
			}
			for _, schema := range foreignSchemas(text, owner) {
				violations = append(violations, fmt.Sprintf("%s: foreign schema %s", set.Position(expr.Pos()), schema))
			}
			return false
		})
	}
	return violations
}

// constantString resolves bounded static string expressions with cycle detection for architecture
// policy inspection.
func constantString(expr ast.Expr, constants map[string]ast.Expr, visiting map[string]bool) (string, bool) {
	switch e := expr.(type) {
	case *ast.BasicLit:
		if e.Kind == token.STRING {
			value, err := strconv.Unquote(e.Value)
			return value, err == nil
		}
	case *ast.ParenExpr:
		return constantString(e.X, constants, visiting)
	case *ast.BinaryExpr:
		if e.Op == token.ADD {
			left, lok := constantString(e.X, constants, visiting)
			right, rok := constantString(e.Y, constants, visiting)
			return left + right, lok && rok
		}
	case *ast.Ident:
		value, ok := constants[e.Name]
		if ok && !visiting[e.Name] {
			visiting[e.Name] = true
			result, ok := constantString(value, constants, visiting)
			delete(visiting, e.Name)
			return result, ok
		}
	}
	return "", false
}
