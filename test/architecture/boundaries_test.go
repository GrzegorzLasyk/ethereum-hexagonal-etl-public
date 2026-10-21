package architecture_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// Inspect production imports without loading external infrastructure or relying
// on the directory from which go test was invoked.
func TestApplicationAndDomainStayTransportIndependent(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate repository")
	}
	root := filepath.Join(filepath.Dir(file), "../..")
	err := filepath.WalkDir(filepath.Join(root, "services"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		slashPath := filepath.ToSlash(path)
		if !strings.Contains(slashPath, "/internal/application/") && !strings.Contains(slashPath, "/internal/domain/") {
			return nil
		}
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, spec := range f.Imports {
			imp, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return err
			}
			if strings.HasPrefix(imp, "google.golang.org/") || strings.Contains(imp, "/gen/") || strings.Contains(imp, "/adapters/") {
				t.Errorf("%s imports transport/infrastructure %s", path, imp)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
