/*
   Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

package emit

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/spec"
)

// GoldenFixture is one verified Java-generated document: the spec.View
// input, exporter assertions, provenance and captured expected output.
// Expected is retained as captured JSON; it is never rebuilt by the emitter.
type GoldenFixture struct {
	spec.View
	ExpectedTemplates *int                   `json:"expectedTemplates"`
	ExpectedError     *string                `json:"expectedError"`
	SchemaVersion     int                    `json:"schemaVersion"`
	Source            string                 `json:"source"`
	Provenance        map[string]interface{} `json:"provenance"`
	Expected          json.RawMessage        `json:"expected"`
}

type fixtureManifest struct {
	SchemaVersion int                    `json:"schemaVersion"`
	Provenance    map[string]interface{} `json:"provenance"`
	Files         map[string]string      `json:"files"`
}

var fixtureFilename = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*\.json$`)
var commitHash = regexp.MustCompile(`^[0-9a-f]{40}$`)

// LoadGoldenFixtures reads a Java-generated corpus for conformance tests
// and runtime comparisons. It fails closed: the manifest is the inventory,
// not an optional report. Dirty provenance is allowed and retained unchanged.
// Map keys are fixture names without the .json suffix.
func LoadGoldenFixtures(dir string) (map[string]GoldenFixture, error) {
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return nil, fmt.Errorf("read golden manifest: %w", err)
	}
	var manifest fixtureManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return nil, fmt.Errorf("decode golden manifest: %w", err)
	}
	if manifest.SchemaVersion != 1 || len(manifest.Files) == 0 {
		return nil, fmt.Errorf("golden manifest must have schemaVersion 1 and a nonempty file inventory")
	}
	ref, _ := manifest.Provenance["hopsworksCommit"].(string)
	if !commitHash.MatchString(ref) || !strings.HasPrefix(ref, HopsworksRef) {
		return nil, fmt.Errorf("golden manifest commit %q does not match emitter reference %s", ref, HopsworksRef)
	}
	for name := range manifest.Files {
		if !fixtureFilename.MatchString(name) || name == "manifest.json" {
			return nil, fmt.Errorf("unsafe golden fixture filename %q", name)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read golden directory: %w", err)
	}
	out := map[string]GoldenFixture{}
	for _, entry := range entries {
		name := entry.Name()
		if name == "manifest.json" || !strings.HasSuffix(name, ".json") {
			continue
		}
		hash, listed := manifest.Files[name]
		if !listed || !entry.Type().IsRegular() {
			return nil, fmt.Errorf("unlisted or non-regular golden fixture %s", name)
		}
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, fmt.Errorf("read golden fixture %s: %w", name, err)
		}
		if fmt.Sprintf("%x", sha256.Sum256(raw)) != hash {
			return nil, fmt.Errorf("golden fixture %s: SHA-256 mismatch", name)
		}
		var f GoldenFixture
		if err := json.Unmarshal(raw, &f); err != nil {
			return nil, fmt.Errorf("decode golden fixture %s: %w", name, err)
		}
		if f.SchemaVersion != 1 || f.Source != "java-generated" {
			return nil, fmt.Errorf("golden fixture %s must have schemaVersion 1 and source java-generated", name)
		}
		if !reflect.DeepEqual(f.Provenance, manifest.Provenance) {
			return nil, fmt.Errorf("golden fixture %s: provenance differs from manifest", name)
		}
		stem := strings.TrimSuffix(name, ".json")
		if f.Name != stem {
			return nil, fmt.Errorf("golden fixture %s: input name %q differs from filename", name, f.Name)
		}
		var expected map[string]interface{}
		if err := json.Unmarshal(f.Expected, &expected); err != nil || len(expected) == 0 {
			return nil, fmt.Errorf("golden fixture %s: missing or invalid expected object", name)
		}
		out[stem] = f
	}
	for name := range manifest.Files {
		if _, ok := out[strings.TrimSuffix(name, ".json")]; !ok {
			return nil, fmt.Errorf("missing golden fixture %s", name)
		}
	}
	return out, nil
}

// GoldenOutputKind separates captured statements from non-execution cases.
type GoldenOutputKind string

const (
	GoldenStatements GoldenOutputKind = "statements"
	GoldenGate       GoldenOutputKind = "gate"
	GoldenDefinition GoldenOutputKind = "definition"
)

// GoldenException preserves the builder's captured gate or exception.
type GoldenException struct {
	Code             string  `json:"code"`
	UserMessage      string  `json:"userMessage"`
	DeveloperMessage *string `json:"developerMessage"`
}

// GoldenOutput is the decoded capture, not an emitter result. Kind alone
// says nothing about execution coverage: even a statements array may be
// empty or contain MySQL-only DTOs.
type GoldenOutput struct {
	Kind       GoldenOutputKind
	Statements []Statement
	Exception  *GoldenException
}

// CapturedOutput decodes Expected without calling Build or changing the
// fixture. Statement order, SQL text and nullable DTO fields are retained.
// Definition-validation details remain in Expected; they are not SQL.
// This checks the output envelope and JSON types, not DTO conformance or
// bindability. LoadGoldenFixtures supplies corpus-integrity verification.
func (f GoldenFixture) CapturedOutput() (GoldenOutput, error) {
	fail := func(reason string) (GoldenOutput, error) {
		return GoldenOutput{}, fmt.Errorf("golden fixture %s: %s", f.Name, reason)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(f.Expected, &fields); err != nil || len(fields) == 0 {
		return fail("missing or invalid expected object")
	}
	gateJSON, ok := fields["gateOrException"]
	if !ok {
		return fail("missing gateOrException field")
	}
	var gate *GoldenException
	if err := json.Unmarshal(gateJSON, &gate); err != nil {
		return fail("invalid gateOrException: " + err.Error())
	}
	statementJSON, hasStatements := fields["statements"]
	if gate != nil {
		if gate.Code == "" {
			return fail("gateOrException has an empty code")
		}
		if f.ExpectedError == nil || *f.ExpectedError != gate.Code {
			return fail("gateOrException code differs from expectedError")
		}
		if hasStatements {
			return fail("gateOrException must not coexist with statements")
		}
		return GoldenOutput{Kind: GoldenGate, Exception: gate}, nil
	}
	if f.ExpectedError != nil {
		return fail("expectedError is set but gateOrException is null")
	}
	if f.Definition != nil {
		if hasStatements {
			return fail("definition-only result must not contain statements")
		}
		return GoldenOutput{Kind: GoldenDefinition}, nil
	}
	if !hasStatements {
		return fail("missing captured statements")
	}
	var captured []*Statement
	if err := json.Unmarshal(statementJSON, &captured); err != nil {
		return fail("invalid captured statements: " + err.Error())
	}
	if captured == nil {
		return fail("captured statements must be an array, not null")
	}
	out := GoldenOutput{Kind: GoldenStatements, Statements: make([]Statement, len(captured))}
	for i, st := range captured {
		if st == nil {
			return fail(fmt.Sprintf("captured statement %d is null", i))
		}
		out.Statements[i] = *st
	}
	return out, nil
}
