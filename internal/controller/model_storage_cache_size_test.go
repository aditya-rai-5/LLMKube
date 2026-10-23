/*
Copyright 2025.

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

package controller

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

func TestMultiFileCachedSize(t *testing.T) {
	requireInitShellEnvironment(t)
	for _, kind := range []string{"http", "hf", "s3", "partially-pinned"} {
		for _, state := range []string{"truncated", "oversized", "matching", "empty", "unknown-size", "head-failed", "offline", "download-failed"} {
			t.Run(kind+"/"+state, func(t *testing.T) {
				content := "complete model artifact"
				cached := "short"
				switch state {
				case "matching", "unknown-size", "head-failed":
					cached = content
				case "oversized":
					cached = content + "extra"
				case "empty":
					content, cached = "", ""
				}
				var gets atomic.Int32
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == http.MethodHead {
						if state == "head-failed" || state == "offline" {
							w.WriteHeader(http.StatusServiceUnavailable)
							return
						}
						if state != "unknown-size" {
							w.Header().Set("Content-Length", strconv.Itoa(len(content)))
						}
						w.WriteHeader(http.StatusOK)
						return
					}
					gets.Add(1)
					if state == "download-failed" || state == "offline" {
						w.WriteHeader(http.StatusServiceUnavailable)
						return
					}
					_, _ = w.Write([]byte(content))
				}))
				t.Cleanup(srv.Close)
				dest := filepath.Join(t.TempDir(), "model.safetensors")
				if err := os.WriteFile(dest, []byte(cached), 0o644); err != nil {
					t.Fatal(err)
				}
				env := []string{"MODEL_FILES=model.safetensors"}
				if kind == "s3" {
					env = append(env, s3TestEnv(srv.URL)...)
				}
				if kind == "partially-pinned" {
					// Pin a second file, leaving the cached shard unpinned.
					env = []string{"MODEL_FILES=model.safetensors\nconfig.json", "MODEL_FILE_SHA256=" + sha256Hex([]byte(content)) + " config.json"}
				}
				script := buildMultiFileInitCommand(true, kind == "s3", kind == "hf", kind == "partially-pinned", RefreshPolicyIfNotPresent)
				out, err := runVerifyScriptEnv(t, script, srv.URL, dest, "", env...)
				if state == "download-failed" || state == "offline" {
					if err == nil {
						t.Fatalf("accepted a truncated cache after download failure: %s", out)
					}
					return
				}
				if err != nil {
					t.Fatalf("init failed: %v\n%s", err, out)
				}
				if got := readOrFail(t, dest); got != content {
					t.Fatalf("cached artifact = %q, want %q", got, content)
				}
				wantGets := int32(1)
				if state == "matching" || state == "empty" {
					wantGets = 0
				}
				if kind == "partially-pinned" {
					wantGets++
				}
				if gets.Load() != wantGets {
					t.Errorf("GET requests = %d, want %d", gets.Load(), wantGets)
				}
				if (state == "unknown-size" || state == "head-failed") && strings.Contains(out, "model.safetensors already cached") {
					t.Errorf("accepted cache without a verified origin size: %s", out)
				}
			})
		}
	}
}

func TestMultiFileOnChangeRejectsUnverifiedCache(t *testing.T) {
	requireInitShellEnvironment(t)
	for _, s3 := range []bool{false, true} {
		for _, pinned := range []bool{false, true} {
			t.Run(fmt.Sprintf("s3-%v/partially-pinned-%v", s3, pinned), func(t *testing.T) {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == http.MethodHead {
						w.Header().Set("Content-Length", "100")
						return
					}
					w.WriteHeader(http.StatusServiceUnavailable)
				}))
				defer srv.Close()
				dest := filepath.Join(t.TempDir(), "model.safetensors")
				if err := os.WriteFile(dest, []byte("short"), 0o644); err != nil {
					t.Fatal(err)
				}
				env := []string{"MODEL_FILES=model.safetensors\nconfig.json"}
				if pinned {
					env = append(env, "MODEL_FILE_SHA256="+sha256Hex([]byte("config"))+" config.json")
				}
				if s3 {
					env = append(env, s3TestEnv(srv.URL)...)
				}
				out, err := runVerifyScriptEnv(t, buildMultiFileInitCommand(true, s3, false, pinned, RefreshPolicyOnChange), srv.URL, dest, "", env...)
				if err == nil {
					t.Fatalf("accepted a truncated cache after failed refresh: %s", out)
				}
				if got := readOrFail(t, dest); got != "short" {
					t.Errorf("failed download overwrote cache with %q", got)
				}
			})
		}
	}
}

func TestMultiFilePublishedSize(t *testing.T) {
	requireInitShellEnvironment(t)
	realMV, err := exec.LookPath("mv")
	if err != nil {
		t.Fatal(err)
	}
	for _, policy := range []string{RefreshPolicyIfNotPresent, RefreshPolicyOnChange} {
		for _, pinned := range []bool{false, true} {
			for _, s3 := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/pinned-%v/s3-%v", policy, pinned, s3), func(t *testing.T) {
					o := newRangeOrigin(t, true)
					dest := filepath.Join(t.TempDir(), "model.safetensors")
					bin := t.TempDir()
					stub := "#!/bin/sh\n\"$REAL_MV\" \"$@\" || exit $?\nif [ \"$2\" = \"$PUBLISH_DEST\" ]; then printf short > \"$2\"; fi\n"
					if err := os.WriteFile(filepath.Join(bin, "mv"), []byte(stub), 0o755); err != nil {
						t.Fatal(err)
					}
					env := []string{"MODEL_FILES=model.safetensors", "PATH=" + bin + ":" + os.Getenv("PATH"), "REAL_MV=" + realMV, "PUBLISH_DEST=" + dest}
					if pinned {
						env = append(env, "MODEL_FILE_SHA256="+sha256Hex(o.content())+" model.safetensors")
					}
					if s3 {
						env = append(env, s3TestEnv(o.srv.URL)...)
					}
					out, err := runVerifyScriptEnv(t, buildMultiFileInitCommand(true, s3, false, pinned, policy), o.srv.URL, dest, "", env...)
					if err == nil {
						t.Fatalf("reported success after a truncated publish: %s", out)
					}
					if got := readOrFail(t, dest); got != "short" {
						t.Errorf("test did not reproduce truncated publication: %q", got)
					}
					if _, err := os.Stat(dest + ".sha256"); !os.IsNotExist(err) {
						t.Error("truncated publication retained a valid checksum stamp")
					}
				})
			}
		}
	}
}
