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
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	inferencev1alpha1 "github.com/defilantech/llmkube/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestMultiFileFUSEStaging(t *testing.T) {
	requireInitShellEnvironment(t)
	realStat, err := exec.LookPath("stat")
	if err != nil {
		t.Fatal(err)
	}
	for _, fs := range []string{"fuse", "fuseblk", "busybox-fuse", "ext4", "unavailable"} {
		for _, prefetch := range []bool{false, true} {
			name := fs + "/serving"
			if prefetch {
				name = fs + "/prefetch"
			}
			t.Run(name, func(t *testing.T) {
				origin := newRangeOrigin(t, true)
				model := &inferencev1alpha1.Model{
					ObjectMeta: metav1.ObjectMeta{Name: "weights", Namespace: "default"},
					Spec:       inferencev1alpha1.ModelSpec{Source: origin.srv.URL, Format: "safetensors", Files: []string{"model.safetensors"}},
					Status:     inferencev1alpha1.ModelStatus{CacheKey: "cache"},
				}
				storage := buildModelStorageConfig(model, nil, "default", true, ModelCacheModeShared, "", defaultPrefetchImage, 0, nil, "")
				containers := storage.initContainers
				if prefetch {
					r := &ModelReconciler{InitContainerImage: defaultPrefetchImage}
					job, err := r.buildPrefetchJob(context.Background(), model, "", "")
					if err != nil {
						t.Fatal(err)
					}
					if job.Spec.PodReplacementPolicy == nil || *job.Spec.PodReplacementPolicy != batchv1.Failed {
						t.Error("prefetch must wait for a failed pod before replacing it")
					}
					containers = job.Spec.Template.Spec.InitContainers
				}
				var downloader *corev1.Container
				for i := range containers {
					if containers[i].Name == "model-downloader" {
						downloader = &containers[i]
					}
				}
				if downloader == nil {
					t.Fatal("missing downloader")
				}
				dir := t.TempDir()
				bin := t.TempDir()
				stub := "#!/bin/sh\nif [ \"$1\" = -f ]; then "
				if fs == "unavailable" {
					stub += "exit 1"
				} else {
					// BusyBox does not name FUSE, but both implementations
					// report the Linux filesystem magic through %t.
					magic, name := "65735546", "fuseblk"
					switch fs {
					case "fuse":
						name = "fuse"
					case "busybox-fuse":
						name = "UNKNOWN"
					case "ext4":
						magic, name = "ef53", "ext2/ext3"
					}
					stub += "if [ \"$3\" = %t ]; then echo " + magic + "; else echo " + name + "; fi"
				}
				stub += "; else exec \"$REAL_STAT\" \"$@\"; fi\n"
				if err := os.WriteFile(filepath.Join(bin, "stat"), []byte(stub), 0o755); err != nil {
					t.Fatal(err)
				}
				sentinel := filepath.Join(dir, "orphan.tmp")
				if err := os.WriteFile(sentinel, []byte("untouched"), 0o644); err != nil {
					t.Fatal(err)
				}
				cmd := exec.Command("sh", "-c", downloader.Command[2])
				cmd.Env = os.Environ()
				// EnvFrom cannot override the explicit serving-pod safety setting.
				cmd.Env = append(cmd.Env, "LLMKUBE_PREFETCH_STAGER=true")
				for _, env := range downloader.Env {
					cmd.Env = append(cmd.Env, env.Name+"="+env.Value)
				}
				cmd.Env = append(cmd.Env, "CACHE_DIR="+dir, "PATH="+bin+":"+os.Getenv("PATH"), "REAL_STAT="+realStat)
				out, err := cmd.CombinedOutput()
				if (!prefetch && fs != "ext4") || fs == "unavailable" {
					if err == nil {
						t.Fatalf("unsafe staging succeeded: %s", out)
					}
					if got := origin.fullFromZero.Load(); got != 0 {
						t.Errorf("unsafe staging issued %d downloads", got)
					}
					if got := readOrFail(t, sentinel); got != "untouched" {
						t.Errorf("guard modified cache: %q", got)
					}
					if _, err := os.Stat(filepath.Join(dir, "model.safetensors")); !os.IsNotExist(err) {
						t.Error("unsafe staging published a file")
					}
					if !strings.Contains(string(out), "ERROR:") {
						t.Errorf("missing diagnostic: %s", out)
					}
				} else {
					if err != nil {
						t.Fatalf("prefetch failed: %v\n%s", err, out)
					}
					if got := readOrFail(t, filepath.Join(dir, "model.safetensors")); got != string(origin.content()) {
						t.Error("prefetch published incorrect bytes")
					}
				}
			})
		}
	}
}
