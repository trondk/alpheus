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

// Command changelog checks CHANGELOG.md, the single source of the release
// notes (GO-65), and makes what is made from it.
//
// Usage:
//
//	go run ./hack/changelog check            fails unless CHANGELOG.md is well formed
//	go run ./hack/changelog notes <version>  prints the release's section, or fails without one
//	go run ./hack/changelog docs             prints docs/changes.md for the website
//
// CHANGELOG.md follows Keep a Changelog: an "## [Unreleased]" section
// first, then one "## [X.Y.Z] - YYYY-MM-DD" section per release, newest
// first, each with the parts in parts, in that order.
package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"
)

// File is the changelog, in the repository's root.
const File = "CHANGELOG.md"

// parts are the parts of every release section, in order.
var parts = []string{"Changes", "Upgrade notes", "CRD changes"}

const unreleased = "Unreleased"

// notReleased marks the Unreleased section on the website.
const notReleased = "*Not released yet: these changes are on `main`, and come with the next release.*"

var (
	versionHeading = regexp.MustCompile(`^## \[(\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?)\] - (\d{4}-\d{2}-\d{2})$`)
	anyHeading     = regexp.MustCompile(`^## `)
)

// section is one "## [...]" section of the changelog.
type section struct {
	name  string // Unreleased or the version
	date  string // YYYY-MM-DD for a release
	line  int    // of the heading, from 1
	lines []string
}

// changelog is the changelog's text before the first section, and its
// sections.
type changelog struct {
	intro    []string
	sections []section
}

func parse(text string) (*changelog, error) {
	c := &changelog{}
	inCode := false
	for i, l := range strings.Split(strings.TrimRight(text, "\n"), "\n") {
		if strings.HasPrefix(l, "```") {
			inCode = !inCode
		}
		if !inCode && anyHeading.MatchString(l) {
			s := section{line: i + 1, lines: []string{l}}
			switch m := versionHeading.FindStringSubmatch(l); {
			case l == "## ["+unreleased+"]":
				s.name = unreleased
			case m != nil:
				if _, err := time.Parse(time.DateOnly, m[2]); err != nil {
					return nil, fmt.Errorf("line %d: %q is not a date", s.line, m[2])
				}
				s.name, s.date = m[1], m[2]
			default:
				return nil, fmt.Errorf("line %d: %q: want \"## [Unreleased]\" or \"## [X.Y.Z] - YYYY-MM-DD\"", s.line, l)
			}
			c.sections = append(c.sections, s)
			continue
		}
		if len(c.sections) == 0 {
			c.intro = append(c.intro, l)
		} else {
			last := &c.sections[len(c.sections)-1]
			last.lines = append(last.lines, l)
		}
	}
	return c, nil
}

// check says what is wrong with the changelog: an Unreleased section
// first, no version twice, and every release section with the three
// parts in order.
func check(text string) error {
	c, err := parse(text)
	if err != nil {
		return err
	}
	var problems []string
	if len(c.sections) == 0 || c.sections[0].name != unreleased {
		problems = append(problems, "the first section must be \"## [Unreleased]\"")
	}
	seen := map[string]bool{}
	for i, s := range c.sections {
		if seen[s.name] {
			problems = append(problems, fmt.Sprintf("line %d: [%s] comes twice", s.line, s.name))
		}
		seen[s.name] = true
		if s.name == unreleased {
			if i > 0 {
				problems = append(problems, fmt.Sprintf("line %d: [Unreleased] must be the first section", s.line))
			}
			continue
		}
		var got []string
		for _, l := range s.lines[1:] {
			if p, ok := strings.CutPrefix(l, "### "); ok {
				got = append(got, p)
			}
		}
		if strings.Join(got, "|") != strings.Join(parts, "|") {
			problems = append(problems, fmt.Sprintf("line %d: [%s] has the parts %q, want %q", s.line, s.name, got, parts))
		}
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "\n"))
	}
	return nil
}

// notes is the section of a release, for RELEASE-NOTES.md. version may
// start with v.
func notes(text, version string) (string, error) {
	if err := check(text); err != nil {
		return "", err
	}
	c, _ := parse(text)
	version = strings.TrimPrefix(version, "v")
	for _, s := range c.sections {
		if s.name == version && s.name != unreleased {
			return strings.TrimRight(strings.Join(s.lines, "\n"), "\n") + "\n", nil
		}
	}
	return "", fmt.Errorf("%s has no \"## [%s] - YYYY-MM-DD\" section: write the release's notes there before releasing it", File, version)
}

// docs is docs/changes.md: the changelog with front matter instead of its
// title, and the Unreleased section marked as planned, since docs/
// describes main (WWW-22).
func docs(text, lastmod string) (string, error) {
	if err := check(text); err != nil {
		return "", err
	}
	c, _ := parse(text)
	var b strings.Builder
	fmt.Fprintf(&b, "---\ntitle: Changes\ndescription: What changed in each version of Alpheus, what to do when upgrading, and what changed in the StreamRun resource.\nweight: 50\nlastmod: %s\n---\n\n", lastmod)
	b.WriteString("<!-- Generated by make generate from CHANGELOG.md; do not edit. -->\n")
	intro := c.intro
	if len(intro) > 0 && strings.HasPrefix(intro[0], "# ") {
		intro = intro[1:]
	}
	b.WriteString(strings.TrimRight(strings.Join(intro, "\n"), "\n") + "\n\n")
	for _, s := range c.sections {
		body := strings.Trim(strings.Join(s.lines, "\n"), "\n")
		if s.name == unreleased {
			heading, rest, _ := strings.Cut(body, "\n")
			body = "{{% planned %}}\n" + heading + "\n\n" + notReleased + "\n" + rest + "\n{{% /planned %}}"
		}
		b.WriteString(body + "\n\n")
	}
	return strings.TrimRight(b.String(), "\n") + "\n", nil
}

// lastmod is when CHANGELOG.md last changed: its last commit's date, or
// today while it has changes that are not committed.
func lastmod() string {
	if exec.Command("git", "diff", "--quiet", "HEAD", "--", File).Run() == nil {
		out, err := exec.Command("git", "log", "-1", "--format=%cs", "--", File).Output()
		if d := strings.TrimSpace(string(out)); err == nil && d != "" {
			return d
		}
	}
	return time.Now().Format(time.DateOnly)
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "changelog:", err)
		os.Exit(1)
	}
}

func run(args []string, w io.Writer) error {
	data, err := os.ReadFile(File)
	if err != nil {
		return err
	}
	text := string(data)
	switch {
	case len(args) == 1 && args[0] == "check":
		if err := check(text); err != nil {
			return fmt.Errorf("%s:\n%w", File, err)
		}
		return nil
	case len(args) == 2 && args[0] == "notes":
		out, err := notes(text, args[1])
		if err == nil {
			_, err = io.WriteString(w, out)
		}
		return err
	case len(args) == 1 && args[0] == "docs":
		out, err := docs(text, lastmod())
		if err == nil {
			_, err = io.WriteString(w, out)
		}
		return err
	}
	return errors.New("usage: changelog check | notes <version> | docs")
}
