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

package main

import (
	"os"
	"strings"
	"testing"
)

const good = `# Changelog

Intro.

## [Unreleased]

### Changes

- Something new.

## [0.2.0] - 2026-12-01

### Changes

- Faster.

### Upgrade notes

- None.

### CRD changes

- None.

## [0.1.0] - 2026-11-20

### Changes

- First.

### Upgrade notes

- None.

### CRD changes

- The StreamRun.
`

// TestCheck accepts a well-formed changelog and refuses one without an
// Unreleased section first, with a release that lacks a part or has them
// out of order, with a version twice, or with a heading that is neither.
func TestCheck(t *testing.T) {
	if err := check(good); err != nil {
		t.Fatalf("a good changelog: %v", err)
	}
	for name, text := range map[string]string{
		"no Unreleased":               strings.Replace(good, "## [Unreleased]\n\n### Changes\n\n- Something new.\n\n", "", 1),
		"Unreleased not first":        strings.Replace(good, "## [Unreleased]", "## [0.3.0] - 2026-12-02\n\n### Changes\n### Upgrade notes\n### CRD changes\n\n## [Unreleased]", 1),
		"a part missing":              strings.Replace(good, "### Upgrade notes\n\n- None.\n\n### CRD changes\n\n- None.", "### CRD changes\n\n- None.", 1),
		"parts out of order":          strings.Replace(good, "### Changes\n\n- First.", "### Upgrade notes\n\n- First.\n\n### Changes", 1),
		"a version twice":             strings.Replace(good, "## [0.1.0] - 2026-11-20", "## [0.2.0] - 2026-11-20", 1),
		"a heading without a version": strings.Replace(good, "## [0.1.0] - 2026-11-20", "## 0.1.0", 1),
		"a date that is not one":      strings.Replace(good, "2026-11-20", "2026-13-40", 1),
	} {
		if err := check(text); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// TestNotes gives a release's section, with or without the v, and fails
// with a message for a version the changelog has no section for, so
// hack/release.sh stops there.
func TestNotes(t *testing.T) {
	for _, v := range []string{"0.2.0", "v0.2.0"} {
		got, err := notes(good, v)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(got, "## [0.2.0] - 2026-12-01\n") || !strings.Contains(got, "- Faster.") ||
			strings.Contains(got, "0.1.0") || strings.Contains(got, "Something new") {
			t.Errorf("notes %s:\n%s", v, got)
		}
	}
	if _, err := notes(good, "v0.3.0"); err == nil || !strings.Contains(err.Error(), "## [0.3.0]") {
		t.Errorf("notes of a version without a section: %v", err)
	}
	if _, err := notes(good, "Unreleased"); err == nil {
		t.Error("notes of Unreleased: accepted")
	}
}

// TestDocs makes docs/changes.md: front matter instead of the title, and
// the Unreleased section, and only that, inside the planned shortcode and
// marked as not released yet.
func TestDocs(t *testing.T) {
	got, err := docs(good, "2026-12-01")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"title: Changes\n", "lastmod: 2026-12-01\n", "{{% planned %}}\n## [Unreleased]\n\n*Not released yet", "- Something new.\n{{% /planned %}}", "\n## [0.2.0] - 2026-12-01"} {
		if !strings.Contains(got, want) {
			t.Errorf("no %q in\n%s", want, got)
		}
	}
	if strings.Contains(got, "# Changelog") || strings.Count(got, "{{% planned %}}") != 1 {
		t.Errorf("docs:\n%s", got)
	}
}

// TestChangelog checks the repository's own CHANGELOG.md, as make ci does.
func TestChangelog(t *testing.T) {
	data, err := os.ReadFile("../../" + File)
	if err != nil {
		t.Fatal(err)
	}
	if err := check(string(data)); err != nil {
		t.Error(err)
	}
}
