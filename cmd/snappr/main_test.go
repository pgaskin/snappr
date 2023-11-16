package main

import (
	"bytes"
	"embed"
	"io/fs"
	"path"
	"strconv"
	"strings"
	"testing"
	_ "time/tzdata"

	"github.com/buildkite/shellwords"
	"github.com/pmezard/go-difflib/difflib"
	"golang.org/x/tools/txtar"
)

//go:embed test/*.txt
var tests embed.FS

const newline = `
`

func Test(t *testing.T) {
	ds, err := fs.ReadDir(tests, "test")
	if err != nil {
		panic(err)
	}
	for _, d := range ds {
		name := d.Name()

		txt, err := fs.ReadFile(tests, path.Join("test", name))
		if err != nil {
			t.Fatal(err)
		}
		arc := txtar.Parse(txt)

		var args, stdin, stdout, stderr []byte
		for _, f := range arc.Files {
			switch f.Name {
			case "args":
				args = f.Data
			case "stdin":
				stdin = f.Data
			case "stdout":
				stdout = f.Data
			case "stderr":
				stderr = f.Data
			}
		}
		if args != nil {
			args = bytes.TrimSuffix(bytes.ReplaceAll(args, []byte(newline), []byte{'\n'}), []byte{'\n'})
		}
		if stdin != nil {
			stdin = bytes.ReplaceAll(stdin, []byte(newline), []byte{'\n'})
		}
		if stdout != nil {
			stdout = bytes.ReplaceAll(stdout, []byte(newline), []byte{'\n'})
		}
		if stderr != nil {
			stderr = bytes.ReplaceAll(stdout, []byte(stderr), []byte{'\n'})
		}

		var status int
		if x, rest, ok := bytes.Cut(args, []byte{':', ' '}); ok {
			if n, err := strconv.ParseUint(string(x), 10, 8); err == nil {
				status = int(n)
				args = rest
			}
		}

		cmd, err := shellwords.Split(string(args))
		if err != nil {
			panic(err)
		}

		t.Run(strings.TrimSuffix(name, ".txt"), func(t *testing.T) {
			t.Log(string(args))

			var actStdout, actStderr bytes.Buffer
			actStatus := Main(cmd, bytes.NewReader(stdin), &actStdout, &actStderr)

			if status != actStatus {
				t.Errorf("incorrect exit status: expected %d, got %d", status, actStatus)
			}
			if stderr != nil && !bytes.Equal(stderr, actStderr.Bytes()) {
				x, err := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
					A:        difflib.SplitLines(string(stderr)),
					B:        difflib.SplitLines(actStderr.String()),
					FromFile: "stderr.expected",
					ToFile:   "stderr.actual",
					Context:  3,
				})
				if err != nil {
					panic(err)
				}
				t.Error(x)
			}
			if stdout != nil && !bytes.Equal(stdout, actStdout.Bytes()) {
				x, err := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
					A:        difflib.SplitLines(string(stdout)),
					B:        difflib.SplitLines(actStdout.String()),
					FromFile: "stdout.expected",
					ToFile:   "stdout.actual",
					Context:  3,
				})
				if err != nil {
					panic(err)
				}
				t.Error(x)
			}
		})
	}
}
