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

package controller

import (
	"context"
	"testing"

	dto "github.com/prometheus/client_model/go"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	foremanv1alpha1 "github.com/defilantech/llmkube/api/foreman/v1alpha1"
	llmkubemetrics "github.com/defilantech/llmkube/internal/metrics"
)

// TestTaskOutcomeLabels verifies the bounded label extraction the operator uses
// to emit terminal-task outcome metrics (#1491): agent name from spec.agentRef,
// kind, verdict, and the machine outcome / elapsedSec / turns carried in the
// result envelope. A missing or unparseable result yields empty outcome and
// zero numeric fields, never a fabricated value. The task NAME is deliberately
// never returned — it is unbounded and would break cardinality.
func TestTaskOutcomeLabels(t *testing.T) {
	t.Run("full envelope with agentRef", func(t *testing.T) {
		task := &foremanv1alpha1.AgenticTask{}
		task.Spec.Kind = foremanv1alpha1.AgenticTaskKindIssueFix
		task.Spec.AgentRef = &corev1.LocalObjectReference{Name: "coder"}
		task.Status.Verdict = foremanv1alpha1.AgenticTaskVerdictNoGo
		task.Status.Result = &runtime.RawExtension{Raw: []byte(
			`{"elapsedSec":42.5,"extra":{"outcome":"ALREADY-RESOLVED","turnCount":17}}`)}

		agent, kind, verdict, outcome, elapsed, turns := taskOutcomeLabels(task)
		if agent != "coder" {
			t.Errorf("agent = %q, want %q", agent, "coder")
		}
		if kind != "issue-fix" {
			t.Errorf("kind = %q, want %q", kind, "issue-fix")
		}
		if verdict != "NO-GO" {
			t.Errorf("verdict = %q, want %q", verdict, "NO-GO")
		}
		if outcome != "ALREADY-RESOLVED" {
			t.Errorf("outcome = %q, want %q", outcome, "ALREADY-RESOLVED")
		}
		if elapsed != 42.5 {
			t.Errorf("elapsed = %f, want 42.5", elapsed)
		}
		if turns != 17 {
			t.Errorf("turns = %d, want 17", turns)
		}
	})

	t.Run("no agentRef and no result envelope", func(t *testing.T) {
		task := &foremanv1alpha1.AgenticTask{}
		task.Spec.Kind = foremanv1alpha1.AgenticTaskKindVerify
		task.Status.Verdict = foremanv1alpha1.AgenticTaskVerdictGatePass

		agent, kind, verdict, outcome, elapsed, turns := taskOutcomeLabels(task)
		if agent != "" {
			t.Errorf("agent = %q, want empty for unset agentRef", agent)
		}
		if kind != "verify" {
			t.Errorf("kind = %q, want %q", kind, "verify")
		}
		if verdict != "GATE-PASS" {
			t.Errorf("verdict = %q, want %q", verdict, "GATE-PASS")
		}
		if outcome != "" || elapsed != 0 || turns != 0 {
			t.Errorf("missing envelope must yield empty/zero, got outcome=%q elapsed=%f turns=%d",
				outcome, elapsed, turns)
		}
	})

	t.Run("unparseable result yields no fabricated values", func(t *testing.T) {
		task := &foremanv1alpha1.AgenticTask{}
		task.Spec.Kind = foremanv1alpha1.AgenticTaskKindFreeform
		task.Spec.AgentRef = &corev1.LocalObjectReference{Name: "freeform-agent"}
		task.Status.Verdict = foremanv1alpha1.AgenticTaskVerdictIncomplete
		task.Status.Result = &runtime.RawExtension{Raw: []byte(`{not-json`)}

		_, _, _, outcome, elapsed, turns := taskOutcomeLabels(task)
		if outcome != "" || elapsed != 0 || turns != 0 {
			t.Errorf("unparseable envelope must yield empty/zero, got outcome=%q elapsed=%f turns=%d",
				outcome, elapsed, turns)
		}
	})
}

// TestTerminalMetricEmittedAtMostOnceOnAuditFailure proves the "exactly once"
// guarantee holds on the audit FAILURE path, not only the happy path (#1491).
// The metric must be emitted only when the dedup marker (the audited
// annotation) is actually persisted by RecordTerminal. When the audit write
// fails the marker is never stamped, so a later reconcile of the same terminal
// task (periodic resync or any update) would otherwise pass the guard and
// increment the counter a second time. A failed audit write therefore means no
// metric (under-count: rates stay correct) rather than a repeating one
// (double-count: inflates exactly the agents that are having trouble).
//
// This test must FAIL against the ordering where the metric is emitted before
// RecordTerminal: there, both reconciles pass the unset-annotation guard and
// each emits, so the counter moves by 2 across the two passes.
func TestTerminalMetricEmittedAtMostOnceOnAuditFailure(t *testing.T) {
	// Foreman-only scheme (no corev1.ConfigMap registered). This is deliberate:
	// audit.RecordTerminal writes its durable record to a ConfigMap, and with
	// the type absent from the scheme that write fails ("no kind registered
	// for v1.ConfigMap"), so RecordTerminal returns an error and never stamps
	// the audited annotation — exactly the audit-failure path under test.
	scheme := runtime.NewScheme()
	if err := foremanv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add foreman scheme: %v", err)
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	task := newTask("dedup-audit-fail")
	task.Spec.Kind = foremanv1alpha1.AgenticTaskKindIssueFix
	task.Spec.AgentRef = &corev1.LocalObjectReference{Name: "dedup-agent"}
	// Terminal state the FleetAgent would have written. A populated result
	// envelope makes the emitted labels deterministic and distinct from the
	// label set used by TestTaskOutcomeLabels.
	finished := metav1.Now()
	task.Status.Phase = foremanv1alpha1.AgenticTaskPhaseSucceeded
	task.Status.Verdict = foremanv1alpha1.AgenticTaskVerdictGo
	task.Status.FinishedAt = &finished
	task.Status.Result = &runtime.RawExtension{Raw: []byte(
		`{"elapsedSec":42.5,"extra":{"outcome":"MODEL-DECIDED","turnCount":17}}`)}
	// No audited annotation: this is the first terminal reconcile.
	if err := fakeClient.Create(context.Background(), task); err != nil {
		t.Fatalf("create task: %v", err)
	}

	r := &AgenticTaskReconciler{Client: fakeClient, Scheme: scheme}
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(task)}

	labels := []string{"dedup-agent", "issue-fix", "GO", "MODEL-DECIDED"}
	readCounter := func() float64 {
		t.Helper()
		var m dto.Metric
		if err := llmkubemetrics.ForemanTaskCompletedTotal.WithLabelValues(labels...).Write(&m); err != nil {
			t.Fatalf("read counter: %v", err)
		}
		return m.GetCounter().GetValue()
	}

	before := readCounter()

	// First terminal reconcile: the audit write fails, so the metric must not
	// be emitted (the dedup marker is never persisted).
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("first reconcile returned an error: %v", err)
	}
	// Second reconcile of the same terminal task (periodic resync / update).
	// The annotation is still unset, so without the fix this pass re-emits.
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("second reconcile returned an error: %v", err)
	}

	after := readCounter()
	if got := after - before; got > 1 {
		t.Errorf("counter must increment at most once across both passes, got +%v (audit write failed both times)", got)
	}
}

func TestTerminalMetricAndAuditOncePerAttempt(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = foremanv1alpha1.AddToScheme(scheme)
	task := newTask("attempt-audit")
	task.Spec.Kind = foremanv1alpha1.AgenticTaskKindIssueFix
	task.Spec.AgentRef = &corev1.LocalObjectReference{Name: "attempt-metric-agent"}
	task.Status.Phase = foremanv1alpha1.AgenticTaskPhaseSucceeded
	task.Status.Verdict = foremanv1alpha1.AgenticTaskVerdictGo
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(task).WithStatusSubresource(task).Build()
	r := &AgenticTaskReconciler{Client: c, Scheme: scheme}
	key := client.ObjectKeyFromObject(task)
	agent, kind, verdict, outcome, _, _ := taskOutcomeLabels(task)
	counter := llmkubemetrics.ForemanTaskCompletedTotal.WithLabelValues(agent, kind, verdict, outcome)
	read := func() float64 {
		t.Helper()
		var metric dto.Metric
		if err := counter.Write(&metric); err != nil {
			t.Fatal(err)
		}
		return metric.GetCounter().GetValue()
	}
	before := read()
	for attempt := int64(1); attempt <= 2; attempt++ {
		if err := c.Get(ctx, key, task); err != nil {
			t.Fatal(err)
		}
		task.Status.Attempt = attempt
		if err := c.Status().Update(ctx, task); err != nil {
			t.Fatal(err)
		}
		for range 2 {
			if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
				t.Fatal(err)
			}
		}
		if got := read() - before; got != float64(attempt) {
			t.Fatalf("completed counter increment = %v, want %d", got, attempt)
		}
	}
	var cms corev1.ConfigMapList
	if err := c.List(ctx, &cms); err != nil {
		t.Fatal(err)
	}
	if len(cms.Items) != 2 {
		t.Fatalf("retained %d audits, want 2", len(cms.Items))
	}
}
