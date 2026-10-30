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

package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	foremanv1alpha1 "github.com/defilantech/llmkube/api/foreman/v1alpha1"
	"github.com/defilantech/llmkube/pkg/foreman/agent/oai"
	"github.com/defilantech/llmkube/pkg/foreman/audit"
)

// Removing the attempt suffix from either writer loses the first run here.
func TestAttemptRecordsSurviveRetry(t *testing.T) {
	for _, recreate := range []bool{false, true} {
		name := "redispatch"
		if recreate {
			name = "recreate"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			task := &foremanv1alpha1.AgenticTask{
				ObjectMeta: metav1.ObjectMeta{Name: "retry", Namespace: "default", UID: "first"},
			}
			c := newRecoveryClient(t, task)
			w := &AgenticTaskWatcher{Client: c, NodeName: "coder"}
			for i := 1; i <= 2; i++ {
				current := getTask(t, c, task.Name)
				if i == 2 && recreate {
					if err := c.Delete(ctx, &current); err != nil {
						t.Fatal(err)
					}
					current = foremanv1alpha1.AgenticTask{
						ObjectMeta: metav1.ObjectMeta{Name: task.Name, Namespace: task.Namespace, UID: "second"},
					}
					if err := c.Create(ctx, &current); err != nil {
						t.Fatal(err)
					}
				}
				current.Status.Phase = foremanv1alpha1.AgenticTaskPhaseScheduled
				current.Status.AssignedNode = "coder"
				if err := c.Status().Update(ctx, &current); err != nil {
					t.Fatal(err)
				}
				if err := w.claim(ctx, &current); err != nil {
					t.Fatal(err)
				}
				verdict, message := foremanv1alpha1.AgenticTaskVerdictIncomplete, "first failure"
				if i == 2 {
					verdict, message = foremanv1alpha1.AgenticTaskVerdictGo, "retry success"
				}
				ref, err := WriteTranscript(ctx, c, &current, &LoopResult{
					Turns: i, Transcript: []oai.Message{{Role: oai.RoleAssistant, Content: message}},
				})
				if err != nil {
					t.Fatal(err)
				}
				res := NewResult("issue-fix", verdict, message, time.Second)
				res.Extra = map[string]any{"transcriptRef": objRefAsMap(ref), "turnCount": i}
				if i == 1 {
					res.FailureReason = foremanv1alpha1.FailureMaxTurnsExhausted
				}
				// Exercise the real run-task producer's JSON hop on the Job path.
				raw, err := json.Marshal(runTaskResultFromResult(res))
				if err != nil {
					t.Fatal(err)
				}
				var emitted RunTaskResult
				if err := json.Unmarshal(raw, &emitted); err != nil {
					t.Fatal(err)
				}
				res = coderJobResultToResult("issue-fix", time.Now(), CoderJobResult{
					Verdict: string(verdict), ResultExtra: emitted.Result.Extra,
					FailureReason: string(res.FailureReason),
				})
				if err := w.patchTerminal(ctx, &current, res, nil); err != nil {
					t.Fatal(err)
				}
				current = getTask(t, c, task.Name)
				if err := audit.RecordTerminal(ctx, c, &current, "audit-system", logr.Discard()); err != nil {
					t.Fatal(err)
				}
				// A second reconcile must not create another record.
				if err := audit.RecordTerminal(ctx, c, &current, "audit-system", logr.Discard()); err != nil {
					t.Fatal(err)
				}
			}
			var records corev1.ConfigMapList
			if err := c.List(ctx, &records, client.MatchingLabels{audit.AuditLabel: "true"}); err != nil {
				t.Fatal(err)
			}
			if len(records.Items) != 2 {
				t.Fatalf("retained %d audit records, want 2", len(records.Items))
			}
			for _, cm := range records.Items {
				checkAttemptRecord(t, c, cm, recreate)
			}
		})
	}
}

func TestClaimAttemptClearsPreviousResultAndRejectsStaleClaim(t *testing.T) {
	ctx := context.Background()
	task := pendingTask("claim-attempt")
	task.Status.Attempt = 1
	task.Status.Result = &runtime.RawExtension{Raw: []byte(`{"verdict":"GO"}`)}
	task.Status.TranscriptRef = "previous"
	task.Status.FinishedAt = task.Status.ClaimedAt
	task.Status.Verdict = foremanv1alpha1.AgenticTaskVerdictGo
	c := newRecoveryClient(t, task)
	w := &AgenticTaskWatcher{Client: c, NodeName: "coder"}
	current := getTask(t, c, task.Name)
	stale := current.DeepCopy()
	if err := w.claim(ctx, &current); err != nil {
		t.Fatal(err)
	}
	if err := w.claim(ctx, stale); !apierrors.IsConflict(err) {
		t.Fatalf("stale claim error = %v, want conflict", err)
	}
	got := getTask(t, c, task.Name)
	if got.Status.Attempt != 2 || got.Status.Result != nil || got.Status.TranscriptRef != "" ||
		got.Status.FinishedAt != nil || got.Status.Verdict != "" {
		t.Fatalf("claim retained old result or lost attempt: %+v", got.Status)
	}
	// Identical timestamp and node must not let an old execution finish a new one.
	old := got.DeepCopy()
	old.Status.Attempt = 1
	result := NewResult("issue-fix", foremanv1alpha1.AgenticTaskVerdictGo, "late", time.Second)
	if err := w.patchTerminal(ctx, old, result, nil); err != nil {
		t.Fatal(err)
	}
	(&NativeAgentLoopExecutor{Client: c}).stampJobName(ctx, old, "late-job")
	got = getTask(t, c, task.Name)
	if got.Status.JobName != "" {
		t.Fatalf("stale Job callback overwrote new attempt: %+v", got.Status)
	}
	if got.Status.Phase != foremanv1alpha1.AgenticTaskPhaseRunning || got.Status.Result != nil {
		t.Fatalf("stale completion overwrote new attempt: %+v", got.Status)
	}
}

func TestRunTaskRejectsSupersededAttempt(t *testing.T) {
	task := pendingTask("delayed-job")
	task.UID = "current"
	task.Status.Attempt = 2
	for _, key := range []string{"current-attempt-1", "deleted-attempt-2"} {
		t.Run(key, func(t *testing.T) {
			c := newRecoveryClient(t, task)
			var out bytes.Buffer
			_, err := RunTask(context.Background(), RunTaskConfig{
				Client: c, Task: client.ObjectKeyFromObject(task), AttemptKey: key, Stdout: &out,
			})
			if err == nil || !strings.Contains(err.Error(), "execution attempt superseded") {
				t.Fatalf("RunTask error = %v", err)
			}
			if !strings.Contains(out.String(), RunTaskSentinelError) {
				t.Fatalf("Job did not emit failure: %s", out.String())
			}
			var cms corev1.ConfigMapList
			if err := c.List(context.Background(), &cms); err != nil {
				t.Fatal(err)
			}
			if len(cms.Items) != 0 {
				t.Fatal("superseded Job wrote records for the new attempt")
			}
		})
	}
}

func checkAttemptRecord(t *testing.T, c client.Client, cm corev1.ConfigMap, recreate bool) {
	t.Helper()
	ctx := context.Background()
	var rec audit.Record
	if err := json.Unmarshal([]byte(cm.Data["audit.json"]), &rec); err != nil {
		t.Fatal(err)
	}
	first := rec.Verdict == "INCOMPLETE"
	if first && rec.FailureReason != "MaxTurnsExhausted" {
		t.Fatalf("lost failure reason: %+v", rec)
	}
	wantAttempt := int64(1)
	if !first && !recreate {
		wantAttempt = 2
	}
	if rec.Attempt != wantAttempt {
		t.Fatalf("attempt = %d, want %d", rec.Attempt, wantAttempt)
	}
	var transcript corev1.ConfigMap
	key := client.ObjectKey{Namespace: rec.Task.Namespace, Name: rec.TranscriptRef}
	if err := c.Get(ctx, key, &transcript); err != nil {
		t.Fatal(err)
	}
	var doc TranscriptDoc
	if err := json.Unmarshal([]byte(transcript.Data[transcriptDataKey]), &doc); err != nil {
		t.Fatal(err)
	}
	want := "retry success"
	if first {
		want = "first failure"
	}
	if len(doc.Messages) != 1 || doc.Messages[0].Content != want {
		t.Fatalf("attempt %s/%d transcript = %+v, want %q", rec.Task.UID, rec.Attempt, doc.Messages, want)
	}
	if len(transcript.OwnerReferences) != 0 {
		t.Fatal("durable transcript is still garbage-collected with the task")
	}
	if transcript.Labels[audit.TranscriptLabel] != "true" {
		t.Fatal("durable transcript is not discoverable by the reaper")
	}
}

// The terminal writer runs only once after Execute returns. A metadata update
// must not lose the result just because the optimistic patch conflicts.
func TestPatchTerminalRetriesConflict(t *testing.T) {
	ctx := context.Background()
	task := pendingTask("terminal-conflict")
	task.Status.Attempt = 1
	base := newRecoveryClient(t, task)
	patched := false
	c := interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string,
			obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption,
		) error {
			if !patched {
				patched = true
				current := getTask(t, base, task.Name)
				current.Annotations = map[string]string{"concurrent": "update"}
				if err := base.Update(ctx, &current); err != nil {
					return err
				}
			}
			return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
	})
	w := &AgenticTaskWatcher{Client: c, NodeName: "coder"}
	result := NewResult("issue-fix", foremanv1alpha1.AgenticTaskVerdictGo, "finished", time.Second)
	if err := w.patchTerminal(ctx, task, result, nil); err != nil {
		t.Errorf("terminal result lost after conflict: %v", err)
	}
	got := getTask(t, base, task.Name)
	if got.Status.Phase != foremanv1alpha1.AgenticTaskPhaseSucceeded || got.Status.Result == nil {
		t.Fatalf("completed execution remains %s without a persisted result", got.Status.Phase)
	}
}

func TestPatchTerminalRejectedResultPreservesAttempt(t *testing.T) {
	for _, superseded := range []bool{false, true} {
		name := "same-attempt"
		if superseded {
			name = "superseded"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			task := pendingTask("terminal-rejected")
			task.Status.Attempt = 1
			base := newRecoveryClient(t, task)
			ref, err := WriteTranscript(ctx, base, task, &LoopResult{Turns: 1,
				Transcript: []oai.Message{{Role: oai.RoleAssistant, Content: "failed attempt evidence"}},
			})
			if err != nil {
				t.Fatal(err)
			}
			patched := false
			c := interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{
				SubResourcePatch: func(ctx context.Context, c client.Client, sub string,
					obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption,
				) error {
					if !patched {
						patched = true
						if superseded {
							current := getTask(t, base, task.Name)
							current.Status.Attempt++
							if err := base.Status().Update(ctx, &current); err != nil {
								return err
							}
						}
						return apierrors.NewInvalid(schema.GroupKind{Group: "foreman.llmkube.dev", Kind: "AgenticTask"},
							task.Name, field.ErrorList{field.NotSupported(field.NewPath("status", "verdict"), "FUTURE", []string{"GO"})})
					}
					return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
				},
			})
			w := &AgenticTaskWatcher{Client: c, NodeName: "coder"}
			result := NewResult("issue-fix", "FUTURE", "future verdict", time.Second)
			result.Extra = map[string]any{"transcriptRef": objRefAsMap(ref)}
			if err := w.patchTerminal(ctx, task, result, nil); err != nil {
				t.Fatal(err)
			}
			got := getTask(t, base, task.Name)
			if superseded {
				if got.Status.Phase != foremanv1alpha1.AgenticTaskPhaseRunning || got.Status.Result != nil {
					t.Fatalf("rejected old result overwrote attempt %d: phase=%s", got.Status.Attempt, got.Status.Phase)
				}
			} else if got.Status.TranscriptRef != ref.Name {
				t.Fatalf("rejected attempt lost transcript reference: got %q, want %q", got.Status.TranscriptRef, ref.Name)
			}
		})
	}
}
