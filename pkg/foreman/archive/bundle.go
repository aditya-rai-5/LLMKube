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

// Package archive writes durable task execution records as immutable on-disk
// bundles for compliance and debugging. Each bundle is a directory containing
// audit.json (the Record), transcript.json (optional), and meta.json.
package archive

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/defilantech/llmkube/pkg/foreman/audit"
)

// BundleSchemaVersion identifies the on-disk bundle layout. Bump it only for
// a change a reader cannot handle transparently.
const BundleSchemaVersion = "foreman.archive.v1"

// metaFile is written last by writeAll, which makes its presence the sentinel
// for a completed bundle. A directory without it is crash debris, not an
// archive. Keep it last.
const metaFile = "meta.json"

// BundleMeta is meta.json. It carries only what the writer can know without
// reaching outside its arguments, so the writer stays a pure function.
type BundleMeta struct {
	SchemaVersion string `json:"schemaVersion"`
	TaskName      string `json:"taskName"`
	RecordedAt    string `json:"recordedAt"`
	HasTranscript bool   `json:"hasTranscript"`
}

// BundleDir returns the directory a record's bundle belongs in.
//
// New attempts use <root>/<repo>/<issue>/<taskName>-<taskUID>-attempt-<n>.
// Legacy records use <root>/<repo>/<issue>/<taskName>-<recordedAt> (or
// <taskName>-<taskUID> if RecordedAt is empty). Repo keeps its slash, so an
// owner/name slug occupies two levels and a prefix listing for one repo works
// naturally; an empty repo is normalized to "no-repo". Colons are stripped
// from the timestamp because they are awkward in paths on some tools and
// filesystems.
//
// Every segment is validated against path traversal and symlink escape.
// Record fields come from a cluster object and an operator could set Repo to
// anything; a bundle must never land outside root. This is the same class of
// defect as issue #1625, where an unvalidated repo slug silently changed which
// base a branch was cut from.
func BundleDir(root string, rec audit.Record) (string, error) {
	if err := validateKey(rec); err != nil {
		return "", err
	}

	if filepath.IsAbs(rec.Repo) {
		return "", fmt.Errorf("archive: repo %q must be relative", rec.Repo)
	}

	issue := "no-issue"
	if rec.Issue != 0 {
		issue = fmt.Sprintf("%d", rec.Issue)
	}

	// Clean, not a literal comparison: "", ".", "./", "./.", "a/.." and
	// "./x/.." all name the same nothing, and any of them slipping past this
	// would drop the repo level entirely and collide with each other under
	// <root>/<issue>/<leaf>.
	repo := rec.Repo
	if filepath.Clean(repo) == "." {
		repo = "no-repo"
	}

	stamp := rec.RecordedAt
	if stamp == "" {
		stamp = rec.Task.UID
	}
	stamp = strings.ReplaceAll(stamp, ":", "-")
	if rec.Attempt > 0 {
		stamp = audit.AttemptKey(rec.Task.UID, rec.Attempt)
	}

	resolvedRoot, err := resolveRoot(root)
	if err != nil {
		return "", err
	}

	// filepath.Join cleans, so a traversing segment shows up as a path that no
	// longer sits under root.
	dir := filepath.Join(resolvedRoot, repo, issue, rec.Task.Name+"-"+stamp)
	if err := contained(resolvedRoot, dir); err != nil {
		return "", err
	}
	return dir, nil
}

// WriteBundle writes audit.json, meta.json, and transcript.json (when
// transcript is non-empty) into the record's bundle directory.
//
// An existing bundle is left untouched, because bundles are immutable. Each
// new execution attempt has its own key. For legacy records, when
// RecordedAt is set a retried task writes a different bundle, since the
// timestamp differs. When the key falls back to Task.UID it does not: a
// Kubernetes object UID is constant for the object's lifetime, so a re-run of
// the same task object reuses the key and the first bundle stands.
//
// "Existing" means complete, not merely present. meta.json is written last, so
// a directory without it is the debris of an interrupted write rather than a
// bundle, and it is removed and rewritten. The alternative, refusing it, would
// reproduce the exact failure this check was hardened against: the debris is
// permanent, so every retry would fail on it and the record would be lost.
// Nothing readable is discarded, because a bundle with no meta.json cannot be
// read as one.
//
// A failed write removes the partial directory, so the next reconcile is not
// suppressed by a half-written bundle that the skip check would mistake for a
// finished one. A partial MkdirAll failure can leave empty parent directories
// behind, but never the leaf, so the retry is not suppressed there either. A
// crash between MkdirAll and the last write, by SIGKILL, eviction or a node
// reboot, gets no cleanup at all and does leave the leaf: that is the case the
// meta.json sentinel covers. Whether the retry then succeeds depends on the
// cause: a permission error or a full disk fails again. What is guaranteed is
// only that it is attempted.
func WriteBundle(root string, rec audit.Record, transcript []byte) error {
	dir, err := BundleDir(root, rec)
	if err != nil {
		return err
	}

	// Lstat, not Stat. Stat resolves a symlink at the bundle path, so a symlink
	// pointing at any existing directory would report IsDir and this function
	// would return nil having archived nothing. That loss is permanent, because
	// the same check fires identically on every retry, and silently dropping a
	// compliance record is worse than the escape the check guards against.
	fi, err := os.Lstat(dir)
	if err == nil {
		if fi.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("archive: bundle path %q is a symlink, not a bundle directory", dir)
		}
		if !fi.IsDir() {
			return fmt.Errorf("archive: bundle path %q exists and is not a directory", dir)
		}
		// The leaf is a real directory, but an intermediate symlink can still
		// have put it outside the root. Without this the containment check
		// never runs at all for a path that already exists.
		if err := verifyContained(root, dir); err != nil {
			return err
		}
		complete, err := isComplete(dir)
		if err != nil {
			return err
		}
		if complete {
			return nil
		}
		// Incomplete: crash debris. Clear it and write the bundle properly.
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("archive: remove incomplete bundle %q: %w", dir, err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("archive: stat bundle dir %q: %w", dir, err)
	}

	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("archive: create bundle dir: %w", err)
	}

	// Re-check containment against the filesystem as it actually is, before any
	// bytes are written: filepath.Join cleans lexically and cannot see a
	// symlink planted on any segment of the path.
	//
	// Known and accepted: this check is path-based, so between it and writeAll
	// another writer on the same mount could swap the leaf for a symlink. The
	// archive root is a shared PVC or hostPath, so the window is real, but
	// closing it needs openat2(RESOLVE_BENEATH) or an O_NOFOLLOW walk held open
	// across every write, which is a larger change than this writer justifies
	// and is not portable to the Metal agent's darwin path. It is recorded here
	// so the next reader knows it was weighed rather than missed.
	if err := verifyContained(root, dir); err != nil {
		_ = os.RemoveAll(dir)
		return err
	}

	if err := writeAll(dir, rec, transcript); err != nil {
		// Leave nothing behind: a partial bundle would look complete to the
		// skip check above and the run would never be archived.
		_ = os.RemoveAll(dir)
		return err
	}
	return nil
}

// isComplete reports whether dir holds a finished bundle, which is to say a
// regular meta.json. Anything else (an empty directory, an audit.json with no
// meta.json, a meta.json that is a symlink or a directory) is the debris of an
// interrupted write and must not be mistaken for an archived record.
func isComplete(dir string) (bool, error) {
	fi, err := os.Lstat(filepath.Join(dir, metaFile))
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("archive: stat %s in %q: %w", metaFile, dir, err)
	}
	return fi.Mode().IsRegular(), nil
}

// resolveRoot returns root as an absolute, symlink-free path. A root that does
// not exist yet is returned absolute but unresolved, which is safe: nothing is
// under it yet, and verifyContained resolves it again once it has been created.
func resolveRoot(root string) (string, error) {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("archive: absolute path for root %q: %w", root, err)
	}
	if _, err := os.Lstat(absRoot); err != nil {
		if os.IsNotExist(err) {
			return absRoot, nil
		}
		return "", fmt.Errorf("archive: stat root %q: %w", absRoot, err)
	}
	resolved, err := filepath.EvalSymlinks(absRoot)
	if err != nil {
		return "", fmt.Errorf("archive: resolve root %q: %w", absRoot, err)
	}
	return resolved, nil
}

// verifyContained resolves dir through any symlinks on its segments and checks
// that what it actually points at still sits under root.
func verifyContained(root, dir string) error {
	resolvedRoot, err := resolveRoot(root)
	if err != nil {
		return err
	}
	resolvedDir, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return fmt.Errorf("archive: resolve bundle path %q: %w", dir, err)
	}
	return contained(resolvedRoot, resolvedDir)
}

// contained reports whether path sits inside resolvedRoot, both of which must
// already be absolute. Every failure is a refusal, never a pass: filepath.Rel
// yields "" on error, and "" would satisfy the ".." test below.
func contained(resolvedRoot, path string) error {
	// Layer the guard. resolveRoot and EvalSymlinks both yield "" alongside
	// their errors, so if either error were ever dropped upstream this function
	// would be handed "" and filepath.Rel(".", ".") would answer ".", nil: a
	// containment check that passes everything. An empty or relative argument
	// here means the caller does not actually know where the path is.
	if !filepath.IsAbs(resolvedRoot) || !filepath.IsAbs(path) {
		return fmt.Errorf("archive: containment needs absolute paths, got root %q and path %q", resolvedRoot, path)
	}
	rel, err := filepath.Rel(resolvedRoot, path)
	if err != nil {
		return fmt.Errorf("archive: cannot relate bundle path %q to root %q: %w", path, resolvedRoot, err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return fmt.Errorf("archive: bundle path %q escapes root %q", path, resolvedRoot)
	}
	return nil
}

// validateKey checks for impossible or unsafe values in the record fields
// that form the bundle path key.
func validateKey(rec audit.Record) error {
	if strings.ContainsRune(rec.Repo, '\x00') {
		return fmt.Errorf("archive: repo contains NUL byte")
	}
	if strings.ContainsRune(rec.Task.Name, '\x00') {
		return fmt.Errorf("archive: task name contains NUL byte")
	}
	if strings.ContainsRune(rec.Task.UID, '\x00') {
		return fmt.Errorf("archive: task UID contains NUL byte")
	}
	if strings.ContainsRune(rec.RecordedAt, '\x00') {
		return fmt.Errorf("archive: recordedAt contains NUL byte")
	}
	if rec.Task.Name == "" {
		return fmt.Errorf("archive: task name is empty; no stable key")
	}
	if rec.RecordedAt == "" && rec.Task.UID == "" {
		return fmt.Errorf("archive: neither recordedAt nor task UID provided; no stable key")
	}
	return nil
}

func writeAll(dir string, rec audit.Record, transcript []byte) error {
	if err := writeJSON(filepath.Join(dir, "audit.json"), rec); err != nil {
		return err
	}
	if len(transcript) > 0 {
		p := filepath.Join(dir, "transcript.json")
		if err := os.WriteFile(p, transcript, 0o640); err != nil {
			return fmt.Errorf("archive: write %s: %w", p, err)
		}
	}
	// meta.json last, always: WriteBundle reads its presence as the proof that
	// this function ran to completion.
	return writeJSON(filepath.Join(dir, metaFile), BundleMeta{
		SchemaVersion: BundleSchemaVersion,
		TaskName:      rec.Task.Name,
		RecordedAt:    rec.RecordedAt,
		HasTranscript: len(transcript) > 0,
	})
}

// writeJSON writes v as two-space-indented JSON with no trailing newline. The
// exact bytes are part of the bundle contract: bundles are immutable and may be
// checksummed by an external compliance reader, so the encoding is pinned by
// TestWriteBundle_JSONByteFormatIsPinned rather than left to the caller's taste.
func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("archive: marshal %s: %w", path, err)
	}
	if err := os.WriteFile(path, b, 0o640); err != nil {
		return fmt.Errorf("archive: write %s: %w", path, err)
	}
	return nil
}
