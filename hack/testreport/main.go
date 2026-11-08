/*
Copyright The Alpheus Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Command testreport makes the end-to-end tests' report pages and serves
// them (hack/report.sh).
//
//	testreport render -root DIR -id ID -events FILE [-log FILE] [-started TIME] [-tests DIR] [-meta key=value]...
//	testreport serve -dir DIR [-listen :8080]
//
// render reads the events of go tool test2json, writes DIR/ID/index.html,
// report.json and go-test.log, and writes DIR/index.html again with every
// report in DIR, newest first. With -tests, each test's description is
// its doc comment in the Go files there. serve serves DIR.
package main

import (
	"bufio"
	"cmp"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"html/template"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fail(errors.New("usage: testreport render|serve [flags]"))
	}
	var err error
	switch os.Args[1] {
	case "render":
		err = render(os.Args[2:])
	case "serve":
		err = serve(os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q: want render or serve", os.Args[1])
	}
	if err != nil {
		fail(err)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "testreport:", err)
	os.Exit(1)
}

// event is one line of go tool test2json.
type event struct {
	Time    time.Time
	Action  string
	Test    string
	Elapsed float64
	Output  string
}

// Test is one test or subtest.
type Test struct {
	Name    string   `json:"name"`
	Doc     string   `json:"description,omitempty"` // the test function's doc comment
	Status  string   `json:"status"`                // pass, fail, skip, or run when it never ended
	Seconds float64  `json:"seconds"`
	Output  []string `json:"-"`
	Subs    []*Test  `json:"subtests,omitempty"`
}

// Report is one run of the tests.
type Report struct {
	ID       string            `json:"id"`
	Result   string            `json:"result"` // pass, fail, or unfinished
	Started  time.Time         `json:"started"`
	Seconds  float64           `json:"seconds"`
	Passed   int               `json:"passed"`
	Failed   int               `json:"failed"`
	Skipped  int               `json:"skipped"`
	Meta     map[string]string `json:"meta"`
	Tests    []*Test           `json:"tests"`
	Package  []string          `json:"-"` // output outside any test, such as a panic
	MetaKeys []string          `json:"-"`
}

type metaFlag map[string]string

func (m metaFlag) String() string { return "" }
func (m metaFlag) Set(v string) error {
	k, val, ok := strings.Cut(v, "=")
	if !ok {
		return fmt.Errorf("-meta %q: want key=value", v)
	}
	m[k] = val
	return nil
}

func render(args []string) error {
	fs := flag.NewFlagSet("render", flag.ContinueOnError)
	root := fs.String("root", "", "the reports' folder")
	id := fs.String("id", "", "this report's name, a folder in root")
	events := fs.String("events", "", "the output of go tool test2json")
	logFile := fs.String("log", "", "go test's own output, kept next to the report")
	started := fs.String("started", "", "when the tests started, RFC 3339; test2json's times are when it ran")
	testsDir := fs.String("tests", "", "the tests' Go files, whose doc comments describe them")
	meta := metaFlag{}
	fs.Var(meta, "meta", "key=value shown at the top; can be repeated")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *root == "" || *id == "" || *events == "" || strings.ContainsAny(*id, `/\`) || strings.HasPrefix(*id, ".") {
		return errors.New("render needs -root, -id (a plain name) and -events")
	}
	f, err := os.Open(*events)
	if err != nil {
		return err
	}
	defer f.Close()
	r, err := parse(f)
	if err != nil {
		return err
	}
	r.ID, r.Meta = *id, meta
	if *testsDir != "" {
		docs, err := testDocs(*testsDir)
		if err != nil {
			return err
		}
		for _, t := range r.Tests {
			t.Doc = docs[t.Name]
		}
	}
	if *started != "" {
		if r.Started, err = time.Parse(time.RFC3339, *started); err != nil {
			return fmt.Errorf("-started: %w", err)
		}
	}
	r.MetaKeys = slices.Sorted(func(yield func(string) bool) {
		for k := range meta {
			if !yield(k) {
				return
			}
		}
	})
	dir := filepath.Join(*root, *id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if *logFile != "" {
		data, err := os.ReadFile(*logFile)
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dir, "go-test.log"), data, 0o644); err != nil {
			return err
		}
	}
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "report.json"), data, 0o644); err != nil {
		return err
	}
	if err := writeTemplate(filepath.Join(dir, "index.html"), reportPage, r); err != nil {
		return err
	}
	return writeIndex(*root)
}

// block is a paragraph of a doc comment, or an indented block kept as it
// is.
type block struct {
	Text string
	Code bool
}

// paragraphs splits a doc comment into its paragraphs and indented blocks.
func paragraphs(doc string) []block {
	var out []block
	for _, p := range strings.Split(doc, "\n\n") {
		if strings.HasPrefix(p, "\t") || strings.HasPrefix(p, "  ") {
			out = append(out, block{Text: p, Code: true})
		} else {
			out = append(out, block{Text: strings.Join(strings.Fields(p), " ")})
		}
	}
	return out
}

// testDocs reads the doc comments of the Test functions in dir's Go
// files, by test name.
func testDocs(dir string) (map[string]string, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*_test.go"))
	if err != nil {
		return nil, err
	}
	docs := map[string]string{}
	fset := token.NewFileSet()
	for _, file := range files {
		f, err := parser.ParseFile(fset, file, nil, parser.ParseComments|parser.SkipObjectResolution)
		if err != nil {
			return nil, err
		}
		for _, d := range f.Decls {
			if fn, ok := d.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Doc != nil && strings.HasPrefix(fn.Name.Name, "Test") {
				doc := strings.TrimSpace(fn.Doc.Text())
				// "TestX runs ..." reads as "Runs ..." under the name.
				if rest, ok := strings.CutPrefix(doc, fn.Name.Name+" "); ok && rest != "" {
					doc = strings.ToUpper(rest[:1]) + rest[1:]
				}
				docs[fn.Name.Name] = doc
			}
		}
	}
	return docs, nil
}

// parse builds the report from test2json's events.
func parse(rd io.Reader) (*Report, error) {
	r := &Report{Result: "unfinished"}
	tests := map[string]*Test{}
	get := func(name string) *Test {
		t := tests[name]
		if t == nil {
			t = &Test{Name: name, Status: "run"}
			tests[name] = t
			if parent, _, ok := strings.Cut(name, "/"); ok && tests[parent] != nil {
				tests[parent].Subs = append(tests[parent].Subs, t)
			} else {
				r.Tests = append(r.Tests, t)
			}
		}
		return t
	}
	sc := bufio.NewScanner(rd)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var e event
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			continue // a line test2json passed through
		}
		if r.Started.IsZero() && !e.Time.IsZero() {
			r.Started = e.Time
		}
		if e.Test == "" {
			switch e.Action {
			case "output":
				r.Package = append(r.Package, e.Output)
			case "pass", "fail":
				r.Result, r.Seconds = e.Action, e.Elapsed
			}
			continue
		}
		t := get(e.Test)
		switch e.Action {
		case "output":
			t.Output = append(t.Output, e.Output)
		case "pass", "fail", "skip":
			t.Status, t.Seconds = e.Action, e.Elapsed
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	var sum float64
	for _, t := range r.Tests {
		sum += t.Seconds
		switch t.Status {
		case "pass":
			r.Passed++
		case "skip":
			r.Skipped++
		default:
			r.Failed++
		}
	}
	r.Seconds = max(r.Seconds, sum)
	return r, nil
}

func writeTemplate(file string, t *template.Template, data any) error {
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	if err := t.Execute(f, data); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// writeIndex lists every report in root, newest first.
func writeIndex(root string) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	var reports []*Report
	for _, en := range entries {
		data, err := os.ReadFile(filepath.Join(root, en.Name(), "report.json"))
		if err != nil {
			continue
		}
		var r Report
		if json.Unmarshal(data, &r) == nil {
			reports = append(reports, &r)
		}
	}
	slices.SortFunc(reports, func(a, b *Report) int { return cmp.Or(b.Started.Compare(a.Started), strings.Compare(b.ID, a.ID)) })
	return writeTemplate(filepath.Join(root, "index.html"), indexPage, reports)
}

func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	dir := fs.String("dir", "/reports", "the reports' folder")
	listen := fs.String("listen", ":8080", "where to serve them")
	if err := fs.Parse(args); err != nil {
		return err
	}
	files := http.FileServerFS(os.DirFS(*dir))
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; img-src data:")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		if r.URL.Path == "/healthz" {
			_, _ = io.WriteString(w, "ok\n")
			return
		}
		files.ServeHTTP(w, r)
	})
	fmt.Fprintf(os.Stderr, "testreport: serving %s on %s\n", *dir, *listen)
	srv := &http.Server{Addr: *listen, Handler: h, ReadHeaderTimeout: 10 * time.Second}
	return srv.ListenAndServe()
}

var funcs = template.FuncMap{
	"dur": func(s float64) string {
		d := time.Duration(s * float64(time.Second)).Round(time.Second)
		if d < time.Second && s > 0 {
			return fmt.Sprintf("%.1fs", s)
		}
		return d.String()
	},
	"mark": func(status string) string {
		switch status {
		case "pass":
			return "✓"
		case "fail":
			return "✗"
		case "skip":
			return "–"
		}
		return "…"
	},
	"join":       func(lines []string) string { return strings.Join(lines, "") },
	"paragraphs": paragraphs,
	"when":       func(t time.Time) string { return t.UTC().Format("2006-01-02 15:04 UTC") },
}

const style = `<style>
:root { --bg: #f4f5f2; --surface: #fff; --text: #1d2326; --muted: #5b6569; --line: #d5dad8; --accent: #0e5f6b; --pass: #2f7d4a; --fail: #b3261e; --code: #eef1ef; color-scheme: light dark; }
@media (prefers-color-scheme: dark) { :root { --bg: #0f1413; --surface: #171d1c; --text: #e3e8e6; --muted: #9aa5a2; --line: #2c3533; --accent: #6cc4cf; --pass: #6fd08f; --fail: #ff8a80; --code: #1d2523; } }
* { box-sizing: border-box; }
body { margin: 0; background: var(--bg); color: var(--text); font: 15px/1.45 system-ui, sans-serif; }
main { max-width: 64rem; margin: 0 auto; padding: 1.2rem 16px 3rem; }
h1 { font-size: 1.4rem; margin: .4rem 0 .8rem; } a { color: var(--accent); }
.muted { color: var(--muted); }
.card { background: var(--surface); border: 1px solid var(--line); border-radius: 6px; padding: 1rem; margin-bottom: 1rem; }
.big { font-size: 1.3rem; font-weight: 700; }
.pass { color: var(--pass); } .fail, .run, .unfinished { color: var(--fail); } .skip { color: var(--muted); }
table { border-collapse: collapse; width: 100%; background: var(--surface); }
th, td { text-align: left; padding: .4rem .6rem; border-bottom: 1px solid var(--line); vertical-align: top; }
th { font-size: .8rem; text-transform: uppercase; letter-spacing: .03em; color: var(--muted); }
dl { display: grid; grid-template-columns: max-content 1fr; gap: .2rem 1rem; margin: .6rem 0 0; } dt { color: var(--muted); } dd { margin: 0; overflow-wrap: anywhere; }
details { background: var(--surface); border: 1px solid var(--line); border-radius: 6px; margin: .4rem 0; }
summary { cursor: pointer; padding: .5rem .8rem; display: flex; gap: .6rem; align-items: baseline; }
summary .name { font-weight: 600; flex: 1; } .sub { margin-left: 1.2rem; }
.doc { padding: .1rem .8rem .3rem; border-top: 1px solid var(--line); } .doc p { margin: .5rem 0; max-width: 46rem; }
.doc pre.code { border: 1px solid var(--line); border-radius: 4px; margin: .5rem 0; }
pre { margin: 0; padding: .7rem .8rem; background: var(--code); border-top: 1px solid var(--line); overflow: auto; max-height: 40rem; font: 12.5px/1.4 ui-monospace, monospace; white-space: pre-wrap; word-break: break-word; }
</style>`

var reportPage = template.Must(template.New("report").Funcs(funcs).Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Test report {{.ID}}</title>` + style + `</head><body><main>
<p><a href="../">← All reports</a></p>
<h1>Test report {{.ID}}</h1>
<div class="card">
  <div class="big {{.Result}}">{{if eq .Result "pass"}}✓ Passed{{else if eq .Result "fail"}}✗ Failed{{else}}… Did not finish{{end}}</div>
  <p>{{.Passed}} passed · {{.Failed}} failed · {{.Skipped}} skipped · {{dur .Seconds}} · started {{when .Started}}</p>
  <dl>{{range .MetaKeys}}<dt>{{.}}</dt><dd>{{index $.Meta .}}</dd>{{end}}</dl>
  <p class="muted">Click a test to see what it does and its own output; failed ones are open. <a href="go-test.log">go test's whole output</a> · <a href="report.json">report.json</a></p>
</div>
{{range .Tests}}{{template "test" .}}{{end}}
{{if .Package}}<details{{if ne .Result "pass"}} open{{end}}><summary><span class="name">Output outside the tests</span></summary><pre>{{join .Package}}</pre></details>{{end}}
</main></body></html>
{{define "test"}}<details{{if ne .Status "pass"}}{{if ne .Status "skip"}} open{{end}}{{end}}>
<summary><span class="{{.Status}}">{{mark .Status}}</span><span class="name">{{.Name}}</span><span class="muted">{{dur .Seconds}}</span></summary>
{{with .Doc}}<div class="doc">{{range paragraphs .}}{{if .Code}}<pre class="code">{{.Text}}</pre>{{else}}<p>{{.Text}}</p>{{end}}{{end}}</div>{{end}}
<pre>{{join .Output}}</pre>
{{range .Subs}}<div class="sub">{{template "test" .}}</div>{{end}}
</details>{{end}}`))

var indexPage = template.Must(template.New("index").Funcs(funcs).Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Test reports</title>` + style + `</head><body><main>
<h1>Alpheus test reports</h1>
<p class="muted">The end-to-end tests on the test cluster, newest first.</p>
{{if .}}<table><thead><tr><th>Started</th><th>Result</th><th>Storage</th><th>Tests</th><th>Duration</th><th>Commit</th></tr></thead><tbody>
{{range .}}<tr><td><a href="{{.ID}}/">{{when .Started}}</a></td>
<td class="{{.Result}}">{{mark .Result}} {{.Result}}</td><td>{{index .Meta "storage"}}</td>
<td>{{.Passed}} passed{{if .Failed}}, <span class="fail">{{.Failed}} failed</span>{{end}}{{if .Skipped}}, {{.Skipped}} skipped{{end}}</td>
<td>{{dur .Seconds}}</td><td><code>{{index .Meta "commit"}}</code></td></tr>{{end}}
</tbody></table>{{else}}<p>No reports yet.</p>{{end}}
</main></body></html>`))
