/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package audit

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	foremanv1alpha1 "github.com/defilantech/llmkube/api/foreman/v1alpha1"
)

func TestWriteRecordCreatesDurableConfigMap(t *testing.T) {
	c := fake.NewClientBuilder().Build()
	rec := Record{SchemaVersion: SchemaVersion, Task: TaskRef{Name: "coder-89", Namespace: "default"}, Verdict: "GO"}

	if err := WriteRecord(context.Background(), c, "default", rec, logr.Discard()); err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}

	var cm corev1.ConfigMap
	key := types.NamespacedName{Namespace: "default", Name: "foreman-audit-coder-89"}
	if err := c.Get(context.Background(), key, &cm); err != nil {
		t.Fatalf("audit ConfigMap not created: %v", err)
	}
	if len(cm.OwnerReferences) != 0 {
		t.Errorf("audit ConfigMap MUST NOT be owner-ref'd (must survive task GC), got %d refs", len(cm.OwnerReferences))
	}
	if cm.Labels[AuditLabel] != "true" || cm.Labels[AuditTaskLabel] != "coder-89" {
		t.Errorf("labels wrong: %v", cm.Labels)
	}
	var got Record
	if err := json.Unmarshal([]byte(cm.Data[auditDataKey]), &got); err != nil {
		t.Fatalf("decode audit.json: %v", err)
	}
	if got.Verdict != "GO" || got.Task.Name != "coder-89" {
		t.Errorf("round-trip wrong: %+v", got)
	}
}

func TestWriteRecordIdempotentUpsert(t *testing.T) {
	c := fake.NewClientBuilder().Build()
	rec := Record{SchemaVersion: SchemaVersion, Task: TaskRef{Name: "t", Namespace: "default"}, Verdict: "GO"}
	ctx := context.Background()
	if err := WriteRecord(ctx, c, "default", rec, logr.Discard()); err != nil {
		t.Fatal(err)
	}
	rec.Verdict = "NO-GO"
	if err := WriteRecord(ctx, c, "default", rec, logr.Discard()); err != nil {
		t.Fatalf("second write: %v", err)
	}
	var cm corev1.ConfigMap
	_ = c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "foreman-audit-t"}, &cm)
	var got Record
	_ = json.Unmarshal([]byte(cm.Data[auditDataKey]), &got)
	if got.Verdict != "NO-GO" {
		t.Errorf("upsert did not update, verdict=%q", got.Verdict)
	}
}

func TestRecordTerminalWritesOnceAndSetsAnnotation(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = foremanv1alpha1.AddToScheme(scheme)

	task := coderTask() // from record_test.go (same package)
	agent := coderAgent()
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(task, agent).
		WithStatusSubresource(task).Build()
	ctx := context.Background()

	if err := RecordTerminal(ctx, c, task, "", logr.Discard()); err != nil {
		t.Fatalf("RecordTerminal: %v", err)
	}
	// audit CM exists in the task namespace (empty audit ns -> task ns)
	var cm corev1.ConfigMap
	key := types.NamespacedName{Namespace: "default", Name: "foreman-audit-coder-89-uid-1-attempt-0"}
	if err := c.Get(ctx, key, &cm); err != nil {
		t.Fatalf("audit CM not written: %v", err)
	}
	// annotation guard set on the task
	var got foremanv1alpha1.AgenticTask
	_ = c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "coder-89"}, &got)
	if got.Annotations[AuditedAnnotation] != "true" {
		t.Errorf("audited annotation not set: %v", got.Annotations)
	}

	// second call is a no-op (already audited): delete the CM, call again,
	// confirm it is NOT recreated.
	_ = c.Delete(ctx, &cm)
	if err := RecordTerminal(ctx, c, &got, "", logr.Discard()); err != nil {
		t.Fatalf("second RecordTerminal: %v", err)
	}
	err := c.Get(ctx, key, &corev1.ConfigMap{})
	if !apierrors.IsNotFound(err) {
		t.Errorf("already-audited task must not re-write the record; got err=%v", err)
	}
}

func TestRecordTerminalLegacyMarkerDoesNotSuppressNewAttempt(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = foremanv1alpha1.AddToScheme(scheme)
	task := coderTask()
	task.Annotations = map[string]string{AuditedAnnotation: "true"}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(task).WithStatusSubresource(task).Build()
	if err := RecordTerminal(ctx, c, task, "", logr.Discard()); err != nil {
		t.Fatal(err)
	}
	var list corev1.ConfigMapList
	if err := c.List(ctx, &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 0 {
		t.Fatal("legacy terminal task was audited again on upgrade")
	}
	task.Status.Attempt = 1
	if err := c.Status().Update(ctx, task); err != nil {
		t.Fatal(err)
	}
	if err := RecordTerminal(ctx, c, task, "", logr.Discard()); err != nil {
		t.Fatal(err)
	}
	if err := c.List(ctx, &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("new attempt has %d audits, want 1", len(list.Items))
	}
}
