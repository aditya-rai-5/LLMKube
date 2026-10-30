/*
Copyright 2026.

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

package archive

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/defilantech/llmkube/pkg/foreman/audit"
)

func testRecord() audit.Record {
	return audit.Record{
		SchemaVersion: "foreman.audit.v1",
		RecordedAt:    "2026-08-23T18:44:22Z",
		Repo:          "defilantech/LLMKube",
		Issue:         1602,
		Verdict:       "GO",
		Task:          audit.TaskRef{Name: "wl-1602-code", Kind: "issue-fix", UID: "task-123"},
	}
}

func TestBundleDir_LayoutAndTimestampSanitising(t *testing.T) {
	// "/arch" is absolute and does not exist, so Abs is a no-op and the root is
	// never symlink-resolved. The full path can therefore be pinned exactly,
	// which keeps the root prefix and the number of path levels under test.
	got, err := BundleDir("/arch", testRecord())
	if err != nil {
		t.Fatalf("BundleDir: %v", err)
	}
	want := "/arch/defilantech/LLMKube/1602/wl-1602-code-2026-08-23T18-44-22Z"
	if got != want {
		t.Errorf("BundleDir = %q, want %q", got, want)
	}
}

func TestBundleDir_ZeroIssueGetsAWellFormedKey(t *testing.T) {
	rec := testRecord()
	rec.Issue = 0
	got, err := BundleDir("/arch", rec)
	if err != nil {
		t.Fatalf("BundleDir: %v", err)
	}
	if !strings.Contains(got, "/no-issue/") {
		t.Errorf("BundleDir = %q, want a no-issue segment", got)
	}
}

func TestBundleDir_RepoThatNamesNothingNormalizesToNoRepo(t *testing.T) {
	// Every one of these cleans to ".". A literal `repo == "" || repo == "."`
	// lets the rest through, which drops the repo level entirely and lands the
	// bundle at <root>/<issue>/<leaf>, where they all collide with each other.
	repos := []string{"", ".", "./", "./.", "a/..", "./x/.."}
	want := "/arch/no-repo/1602/wl-1602-code-2026-08-23T18-44-22Z"
	for _, repo := range repos {
		t.Run("repo="+repo, func(t *testing.T) {
			rec := testRecord()
			rec.Repo = repo
			got, err := BundleDir("/arch", rec)
			if err != nil {
				t.Fatalf("BundleDir: %v", err)
			}
			if got != want {
				t.Errorf("BundleDir(repo=%q) = %q, want %q", repo, got, want)
			}
		})
	}
}

func TestBundleDir_RefusesToEscapeTheRoot(t *testing.T) {
	cases := map[string]string{
		"traversing repo": "../../etc",
		"absolute repo":   "/etc/passwd",
		"dot-dot segment": "a/../../b",
	}
	for name, repo := range cases {
		t.Run(name, func(t *testing.T) {
			rec := testRecord()
			rec.Repo = repo
			if _, err := BundleDir("/arch", rec); err == nil {
				t.Errorf("BundleDir(repo=%q) = nil error, want a refusal", repo)
			}
		})
	}
}

func TestBundleDir_EmptyRecordedAtFallsBackToUID(t *testing.T) {
	rec := testRecord()
	rec.RecordedAt = ""
	rec.Task.UID = "stable-uid-123"
	got, err := BundleDir("/arch", rec)
	if err != nil {
		t.Fatalf("BundleDir: %v", err)
	}
	if !strings.Contains(got, "stable-uid-123") {
		t.Errorf("BundleDir with empty RecordedAt = %q, want to contain UID", got)
	}
}

func TestBundleDir_RejectsEmptyRecordedAtAndUID(t *testing.T) {
	rec := testRecord()
	rec.RecordedAt = ""
	rec.Task.UID = ""
	if _, err := BundleDir("/arch", rec); err == nil {
		t.Fatal("BundleDir with no RecordedAt or UID = nil error, want a rejection")
	}
}

func TestBundleDir_RejectsEmptyTaskName(t *testing.T) {
	rec := testRecord()
	rec.Task.Name = ""
	if _, err := BundleDir("/arch", rec); err == nil {
		t.Fatal("BundleDir with an empty task name = nil error, want a rejection; " +
			"the key would degenerate to a leading-dash segment such as -2026-08-23T18-44-22Z")
	}
}

func TestBundleDir_RejectsNULInKeyFields(t *testing.T) {
	// Every field that becomes a path component must be checked: a NUL reaches
	// the syscall layer and fails there with an opaque "invalid argument",
	// which is exactly what this validation exists to turn into a real message.
	cases := map[string]func(*audit.Record){
		"repo":       func(r *audit.Record) { r.Repo = "foo\x00bar" },
		"task name":  func(r *audit.Record) { r.Task.Name = "task\x00name" },
		"task UID":   func(r *audit.Record) { r.RecordedAt = ""; r.Task.UID = "uid\x00evil" },
		"recordedAt": func(r *audit.Record) { r.RecordedAt = "2026-08-23T18:44:22\x00Z" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			rec := testRecord()
			mutate(&rec)
			if _, err := BundleDir("/arch", rec); err == nil {
				t.Errorf("BundleDir with a NUL byte in %s = nil error, want a rejection", name)
			}
		})
	}
}

func TestWriteBundle_WritesAuditTranscriptAndMeta(t *testing.T) {
	root := t.TempDir()
	if err := WriteBundle(root, testRecord(), []byte(`{"truncated":true}`)); err != nil {
		t.Fatalf("WriteBundle: %v", err)
	}
	dir, err := BundleDir(root, testRecord())
	if err != nil {
		t.Fatalf("BundleDir: %v", err)
	}

	var rec audit.Record
	readJSON(t, filepath.Join(dir, "audit.json"), &rec)
	if rec.Task.Name != "wl-1602-code" || rec.Verdict != "GO" {
		t.Errorf("audit.json round-trip = %+v, want the record we wrote", rec)
	}

	tr, err := os.ReadFile(filepath.Join(dir, "transcript.json"))
	if err != nil {
		t.Fatalf("read transcript.json: %v", err)
	}
	if string(tr) != `{"truncated":true}` {
		t.Errorf("transcript.json = %q, want the bytes verbatim including the truncated flag", tr)
	}

	var meta BundleMeta
	readJSON(t, filepath.Join(dir, "meta.json"), &meta)
	if meta.SchemaVersion != "foreman.archive.v1" {
		t.Errorf("meta.schemaVersion = %q, want literal \"foreman.archive.v1\"", meta.SchemaVersion)
	}
	if !meta.HasTranscript {
		t.Error("meta.hasTranscript = false, want true")
	}
	if meta.TaskName != "wl-1602-code" {
		t.Errorf("meta.taskName = %q, want wl-1602-code", meta.TaskName)
	}
	if meta.RecordedAt != "2026-08-23T18:44:22Z" {
		t.Errorf("meta.recordedAt = %q, want 2026-08-23T18:44:22Z", meta.RecordedAt)
	}
}

func TestWriteBundle_JSONByteFormatIsPinned(t *testing.T) {
	// The bundle is immutable and an external reader may checksum it, so the
	// on-disk encoding is part of the contract, not an implementation detail.
	// Without this, swapping MarshalIndent for Marshal survives the suite.
	root := t.TempDir()
	if err := WriteBundle(root, testRecord(), []byte(`{"x":1}`)); err != nil {
		t.Fatalf("WriteBundle: %v", err)
	}
	dir, err := BundleDir(root, testRecord())
	if err != nil {
		t.Fatalf("BundleDir: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		t.Fatalf("read meta.json: %v", err)
	}
	want := `{
  "schemaVersion": "foreman.archive.v1",
  "taskName": "wl-1602-code",
  "recordedAt": "2026-08-23T18:44:22Z",
  "hasTranscript": true
}`
	if string(got) != want {
		t.Errorf("meta.json bytes =\n%s\n\nwant\n%s", got, want)
	}

	auditBytes, err := os.ReadFile(filepath.Join(dir, "audit.json"))
	if err != nil {
		t.Fatalf("read audit.json: %v", err)
	}
	if !strings.HasPrefix(string(auditBytes), "{\n  \"schemaVersion\": \"foreman.audit.v1\",\n") {
		t.Errorf("audit.json is not two-space-indented JSON:\n%s", auditBytes)
	}
}

func TestWriteBundle_NoTranscriptStillArchivesTheRecord(t *testing.T) {
	root := t.TempDir()
	if err := WriteBundle(root, testRecord(), nil); err != nil {
		t.Fatalf("WriteBundle: %v", err)
	}
	dir, err := BundleDir(root, testRecord())
	if err != nil {
		t.Fatalf("BundleDir: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "audit.json")); err != nil {
		t.Errorf("audit.json missing for a transcript-less run: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "transcript.json")); !os.IsNotExist(err) {
		t.Errorf("transcript.json exists for a run that had none (err=%v)", err)
	}
	var meta BundleMeta
	readJSON(t, filepath.Join(dir, "meta.json"), &meta)
	if meta.HasTranscript {
		t.Error("meta.hasTranscript = true for a run with no transcript")
	}
}

func TestWriteBundle_ExistingBundleIsNotRewritten(t *testing.T) {
	root := t.TempDir()
	if err := WriteBundle(root, testRecord(), []byte(`original`)); err != nil {
		t.Fatalf("first WriteBundle: %v", err)
	}
	if err := WriteBundle(root, testRecord(), []byte(`REPLACED`)); err != nil {
		t.Fatalf("second WriteBundle: %v", err)
	}
	dir, err := BundleDir(root, testRecord())
	if err != nil {
		t.Fatalf("BundleDir: %v", err)
	}
	tr, err := os.ReadFile(filepath.Join(dir, "transcript.json"))
	if err != nil {
		t.Fatalf("read transcript.json: %v", err)
	}
	if string(tr) != "original" {
		t.Errorf("transcript.json = %q, want the first write preserved; bundles are immutable", tr)
	}
}

func TestWriteBundle_StrayFileRefusesBundle(t *testing.T) {
	root := t.TempDir()
	rec := testRecord()
	dir, err := BundleDir(root, rec)
	if err != nil {
		t.Fatalf("BundleDir: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(dir), 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(dir, []byte("stray"), 0o640); err != nil {
		t.Fatalf("create stray file: %v", err)
	}

	if err := WriteBundle(root, rec, nil); err == nil {
		t.Fatal("WriteBundle with stray regular file at path = nil error, want error")
	}
}

func TestWriteBundle_PartialWriteLeavesNoBundleSoItRetries(t *testing.T) {
	root := t.TempDir()
	rec := testRecord()
	// json.Marshal refuses a non-finite float, so the bundle directory is
	// created and then audit.json fails to write. That is the only path that
	// exercises the cleanup, and without it a half-written bundle would look
	// complete to the skip check and never be retried.
	rec.ElapsedSec = math.Inf(1)

	if err := WriteBundle(root, rec, nil); err == nil {
		t.Fatal("WriteBundle with a non-finite ElapsedSec = nil error, want a marshal failure")
	}
	dir, err := BundleDir(root, rec)
	if err != nil {
		t.Fatalf("BundleDir: %v", err)
	}
	if _, err := os.Stat(dir); err == nil {
		t.Error("a failed write left a bundle directory behind, which would suppress the retry")
	}
}

func TestWriteBundle_UnwritableRootFails(t *testing.T) {
	// A regular file standing where the root directory should be, rather than
	// a chmod 0500 directory. Root ignores POSIX permission bits, so a mode-
	// based unwritable directory is writable for uid 0 and this assertion
	// inverted under any root container: CI runs unprivileged and stayed
	// green while the Foreman gate Job, which runs as root, failed every run.
	// ENOTDIR is refused for every uid, so the test now means the same thing
	// wherever it runs.
	root := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(root, []byte("x"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if err := WriteBundle(root, testRecord(), nil); err == nil {
		t.Fatal("WriteBundle into a root that is not a directory = nil error, want a failure")
	}
}

func TestWriteBundle_FileModes(t *testing.T) {
	root := t.TempDir()
	if err := WriteBundle(root, testRecord(), []byte("test")); err != nil {
		t.Fatalf("WriteBundle: %v", err)
	}
	dir, err := BundleDir(root, testRecord())
	if err != nil {
		t.Fatalf("BundleDir: %v", err)
	}

	dirInfo, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat bundle dir: %v", err)
	}
	if dirMode := dirInfo.Mode().Perm(); dirMode != 0o750 {
		t.Errorf("bundle dir mode = %03o, want 0750", dirMode)
	}

	// This record was written with a transcript, so all three files must exist.
	// Skipping a missing one would let a lost file pass as a clean loop.
	for _, name := range []string{"audit.json", "transcript.json", "meta.json"} {
		path := filepath.Join(dir, name)
		fi, err := os.Stat(path)
		if err != nil {
			t.Errorf("stat %s: %v", name, err)
			continue
		}
		if fileMode := fi.Mode().Perm(); fileMode != 0o640 {
			t.Errorf("%s mode = %03o, want 0640", name, fileMode)
		}
	}
}

// TestWriteBundle_RefusesASymlinkAtTheBundlePath covers the skip check reading
// through a symlink. The symlink target is inside the archive root, so the
// containment check alone cannot catch it: only refusing a non-directory at the
// bundle path does. os.Stat here means WriteBundle returns nil having archived
// nothing, permanently, because the retry hits the same check.
func TestWriteBundle_RefusesASymlinkAtTheBundlePath(t *testing.T) {
	root := t.TempDir()
	rec := testRecord()
	dir, err := BundleDir(root, rec)
	if err != nil {
		t.Fatalf("BundleDir: %v", err)
	}

	decoy := filepath.Join(root, "decoy")
	if err := os.Mkdir(decoy, 0o750); err != nil {
		t.Fatalf("mkdir decoy: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(dir), 0o750); err != nil {
		t.Fatalf("mkdir bundle parents: %v", err)
	}
	mustSymlink(t, decoy, dir)

	if err := WriteBundle(root, rec, []byte(`{"x":1}`)); err == nil {
		t.Fatal("WriteBundle onto a symlinked bundle path = nil error; " +
			"the record was silently dropped and every retry drops it too")
	}
	entries, err := os.ReadDir(decoy)
	if err != nil {
		t.Fatalf("read decoy: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("wrote %d entries through the symlink into %q, want none", len(entries), decoy)
	}
}

// TestWriteBundle_RefusesASymlinkedSegmentHoldingTheBundle covers the shape the
// leaf check cannot see: an intermediate segment is a symlink whose target
// already holds the matching sub-path, so the leaf really is a directory and
// the write is skipped without the containment check ever running.
func TestWriteBundle_RefusesASymlinkedSegmentHoldingTheBundle(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	rec := testRecord()
	dir, err := BundleDir(root, rec)
	if err != nil {
		t.Fatalf("BundleDir: %v", err)
	}

	// Reproduce the bundle's sub-path under the symlink target, so a stat
	// through the symlink reports a finished bundle.
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatalf("resolve root: %v", err)
	}
	rel, err := filepath.Rel(filepath.Join(resolvedRoot, "defilantech"), dir)
	if err != nil {
		t.Fatalf("rel: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(outside, rel), 0o750); err != nil {
		t.Fatalf("mkdir outside: %v", err)
	}
	mustSymlink(t, outside, filepath.Join(root, "defilantech"))

	if err := WriteBundle(root, rec, []byte(`{"x":1}`)); err == nil {
		t.Fatal("WriteBundle through a symlinked segment holding the bundle = nil error, want a refusal")
	}
	assertNoFilesUnder(t, outside)
}

// TestWriteBundle_RefusesASymlinkedSegment is the plain escape: an intermediate
// symlink sends MkdirAll outside the archive root entirely.
func TestWriteBundle_RefusesASymlinkedSegment(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()

	mustSymlink(t, outside, filepath.Join(root, "defilantech"))

	if err := WriteBundle(root, testRecord(), []byte(`{"x":1}`)); err == nil {
		t.Fatal("WriteBundle through a symlinked segment = nil error, want a refusal")
	}
	assertNoFilesUnder(t, outside)
}

// TestVerifyContained_RefusesAPathItCannotResolve pins the failure direction of
// the containment verdict. filepath.EvalSymlinks and filepath.Rel both return a
// value that passes a naive check when they fail (Rel returns ""), so
// discarding their errors turns the security check of this file into a no-op.
func TestVerifyContained_RefusesAPathItCannotResolve(t *testing.T) {
	root := t.TempDir()
	if err := verifyContained(root, filepath.Join(root, "never-created")); err == nil {
		t.Fatal("verifyContained on a path it cannot resolve = nil error; " +
			"a containment check that cannot answer must refuse, never pass")
	}
}

// TestContained_RefusesWhenItCannotRelateThePaths forces the filepath.Rel
// failure on its own. Rel yields "" alongside its error, and "" is neither ".."
// nor ".."-prefixed, so discarding that error turns the escape test into a
// guaranteed pass.
func TestContained_RefusesWhenItCannotRelateThePaths(t *testing.T) {
	if err := contained("/arch", "relative/path"); err == nil {
		t.Fatal("contained on paths it cannot relate = nil error, want a refusal")
	}
}

func TestVerifyContained_RefusesAnUnresolvableRoot(t *testing.T) {
	base := t.TempDir()
	loop := filepath.Join(base, "loop")
	mustSymlink(t, loop, loop)

	// The bundle path given here resolves cleanly, so root resolution is the
	// only thing that can fail. Passing a path under the loop instead would let
	// this pass through the EvalSymlinks(dir) guard without ever reaching the
	// root.
	if err := verifyContained(loop, base); err == nil {
		t.Fatal("verifyContained with an unresolvable root = nil error, want a refusal")
	}
}

func TestResolveRoot_RefusesARootItCannotResolve(t *testing.T) {
	base := t.TempDir()
	loop := filepath.Join(base, "loop")
	mustSymlink(t, loop, loop)

	if _, err := resolveRoot(loop); err == nil {
		t.Fatal("resolveRoot on a symlink loop = nil error; EvalSymlinks returns \"\" " +
			"with its error, and \"\" as a root passes every containment test")
	}
}

// TestContained_RefusesNonAbsolutePaths pins the layered guard. resolveRoot and
// EvalSymlinks both hand back "" with their errors, so if either error were
// dropped upstream this function would be asked to relate "" to "", which
// filepath.Rel answers with ".", nil: containment would pass for everything.
func TestContained_RefusesNonAbsolutePaths(t *testing.T) {
	cases := map[string][2]string{
		"both empty":    {"", ""},
		"empty root":    {"", "/arch/bundle"},
		"empty path":    {"/arch", ""},
		"relative root": {"arch", "/arch/bundle"},
	}
	for name, pair := range cases {
		t.Run(name, func(t *testing.T) {
			if err := contained(pair[0], pair[1]); err == nil {
				t.Errorf("contained(%q, %q) = nil error, want a refusal", pair[0], pair[1])
			}
		})
	}
}

// TestWriteBundle_IncompleteDirectoryIsRewritten covers the failure mode of
// Critical 1 reached without a symlink. A crash between MkdirAll and the last
// write leaves a directory with no cleanup, and treating any directory as a
// finished bundle drops the record on that reconcile and on every one after it.
func TestWriteBundle_IncompleteDirectoryIsRewritten(t *testing.T) {
	debris := map[string][]string{
		"empty leaf":               nil,
		"audit.json but no meta":   {"audit.json"},
		"audit and transcript":     {"audit.json", "transcript.json"},
		"meta.json is a directory": {"meta.json/"},
	}
	for name, files := range debris {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			rec := testRecord()
			dir, err := BundleDir(root, rec)
			if err != nil {
				t.Fatalf("BundleDir: %v", err)
			}
			if err := os.MkdirAll(dir, 0o750); err != nil {
				t.Fatalf("mkdir leaf: %v", err)
			}
			for _, f := range files {
				if strings.HasSuffix(f, "/") {
					if err := os.Mkdir(filepath.Join(dir, strings.TrimSuffix(f, "/")), 0o750); err != nil {
						t.Fatalf("mkdir %s: %v", f, err)
					}
					continue
				}
				if err := os.WriteFile(filepath.Join(dir, f), []byte("debris"), 0o640); err != nil {
					t.Fatalf("write %s: %v", f, err)
				}
			}

			if err := WriteBundle(root, rec, []byte(`{"truncated":true}`)); err != nil {
				t.Fatalf("WriteBundle over %s: %v", name, err)
			}

			var meta BundleMeta
			readJSON(t, filepath.Join(dir, metaFile), &meta)
			if meta.SchemaVersion != BundleSchemaVersion || !meta.HasTranscript {
				t.Errorf("meta.json = %+v, want a completed bundle", meta)
			}
			tr, err := os.ReadFile(filepath.Join(dir, "transcript.json"))
			if err != nil {
				t.Fatalf("read transcript.json: %v", err)
			}
			if string(tr) != `{"truncated":true}` {
				t.Errorf("transcript.json = %q, want the record we passed; the debris was not replaced", tr)
			}
		})
	}
}

// TestWriteBundle_RepeatedReconcilesOverDebrisDoNotLoseTheRecord is the shape
// the reviewer ran: three consecutive reconciles against a pre-created empty
// leaf. All three returning nil means the record is gone for good.
func TestWriteBundle_RepeatedReconcilesOverDebrisDoNotLoseTheRecord(t *testing.T) {
	root := t.TempDir()
	rec := testRecord()
	dir, err := BundleDir(root, rec)
	if err != nil {
		t.Fatalf("BundleDir: %v", err)
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("mkdir leaf: %v", err)
	}

	for i := 1; i <= 3; i++ {
		if err := WriteBundle(root, rec, []byte(`original`)); err != nil {
			t.Fatalf("reconcile %d: %v", i, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, metaFile)); err != nil {
		t.Fatalf("no bundle after three reconciles over an empty leaf: %v", err)
	}
	// The first reconcile completed the bundle; the two after it must respect
	// immutability rather than rewriting it.
	tr, err := os.ReadFile(filepath.Join(dir, "transcript.json"))
	if err != nil {
		t.Fatalf("read transcript.json: %v", err)
	}
	if string(tr) != "original" {
		t.Errorf("transcript.json = %q, want the completed bundle left alone", tr)
	}
}

// TestWriteAll_MetaIsWrittenLast pins the ordering the sentinel depends on. If
// meta.json were written first, an interrupted write would leave a directory
// that WriteBundle reads as a finished bundle, which is the whole defect.
func TestWriteAll_MetaIsWrittenLast(t *testing.T) {
	dir := t.TempDir()
	rec := testRecord()
	rec.ElapsedSec = math.Inf(1) // audit.json, the first write, fails to marshal

	if err := writeAll(dir, rec, []byte(`{"x":1}`)); err == nil {
		t.Fatal("writeAll with a non-finite ElapsedSec = nil error, want a marshal failure")
	}
	if _, err := os.Lstat(filepath.Join(dir, metaFile)); !os.IsNotExist(err) {
		t.Errorf("%s exists after an interrupted writeAll (err=%v); it is the completion "+
			"sentinel and must be written last", metaFile, err)
	}
}

func mustSymlink(t *testing.T, target, link string) {
	t.Helper()
	if err := os.Symlink(target, link); err != nil {
		// Not a skip. These are the only regression tests for the symlink
		// escape, and a silent skip reads exactly like a pass.
		t.Fatalf("symlink %q -> %q: %v; this platform cannot run the symlink "+
			"containment regression tests, which must not be mistaken for them passing", link, target, err)
	}
}

func assertNoFilesUnder(t *testing.T, dir string) {
	t.Helper()
	var leaked []string
	if err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			leaked = append(leaked, p)
		}
		return nil
	}); err != nil {
		t.Fatalf("walk %s: %v", dir, err)
	}
	if len(leaked) != 0 {
		t.Errorf("files written outside the archive root: %v", leaked)
	}
}

func readJSON(t *testing.T, path string, v any) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatalf("unmarshal %s: %v", path, err)
	}
}

func TestWriteBundleSeparateAttemptsWithSameTimestamp(t *testing.T) {
	root := t.TempDir()
	rec := testRecord()
	rec.Attempt = 1
	if err := WriteBundle(root, rec, []byte(`{"attempt":1}`)); err != nil {
		t.Fatal(err)
	}
	first, err := BundleDir(root, rec)
	if err != nil {
		t.Fatal(err)
	}
	rec.Attempt = 2
	if err := WriteBundle(root, rec, []byte(`{"attempt":2}`)); err != nil {
		t.Fatal(err)
	}
	second, err := BundleDir(root, rec)
	if err != nil {
		t.Fatal(err)
	}
	for i, dir := range []string{first, second} {
		body, err := os.ReadFile(filepath.Join(dir, "transcript.json"))
		if err != nil {
			t.Fatal(err)
		}
		want := `{"attempt":1}`
		if i == 1 {
			want = `{"attempt":2}`
		}
		if string(body) != want {
			t.Fatalf("bundle %s = %s, want %s", dir, body, want)
		}
	}
}
