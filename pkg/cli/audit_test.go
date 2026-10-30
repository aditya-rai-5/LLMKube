/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package cli

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/defilantech/llmkube/pkg/foreman/audit"
)

func auditCM(name, recordedAt, repo, verdict string) *corev1.ConfigMap {
	rec := audit.Record{
		SchemaVersion: audit.SchemaVersion,
		RecordedAt:    recordedAt,
		Task:          audit.TaskRef{Name: name, Namespace: "default"},
		Repo:          repo, Verdict: verdict,
	}
	data, _ := json.Marshal(rec)
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: "foreman-audit-" + name, Namespace: "default",
			Labels: map[string]string{audit.AuditLabel: "true", audit.AuditTaskLabel: name},
		},
		Data: map[string]string{"audit.json": string(data)},
	}
}

func TestExportAuditRecordsOrderedAndFiltered(t *testing.T) {
	c := fake.NewClientBuilder().WithObjects(
		auditCM("b", "2026-06-25T02:00:00Z", "defilantech/LLMKube", "GO"),
		auditCM("a", "2026-06-25T01:00:00Z", "defilantech/LLMKube", "NO-GO"),
		auditCM("c", "2026-06-25T03:00:00Z", "other/repo", "GO"),
	).Build()

	var buf strings.Builder
	if err := exportAuditRecords(context.Background(), c, "default", "", "", &buf); err != nil {
		t.Fatalf("export: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("want 3 JSONL lines, got %d: %q", len(lines), buf.String())
	}
	if !strings.Contains(lines[0], `"name":"a"`) || !strings.Contains(lines[2], `"name":"c"`) {
		t.Errorf("not ordered by recordedAt: %v", lines)
	}

	var buf2 strings.Builder
	if err := exportAuditRecords(context.Background(), c, "default", "defilantech/LLMKube", "", &buf2); err != nil {
		t.Fatal(err)
	}
	if n := len(strings.Split(strings.TrimSpace(buf2.String()), "\n")); n != 2 {
		t.Errorf("repo filter: want 2, got %d", n)
	}

	var buf3 strings.Builder
	if err := exportAuditRecords(context.Background(), c, "default", "", "2026-06-25T02:00:00Z", &buf3); err != nil {
		t.Fatal(err)
	}
	if n := len(strings.Split(strings.TrimSpace(buf3.String()), "\n")); n != 2 {
		t.Errorf("since filter: want 2, got %d", n)
	}
}

func TestExportAuditPreservesAttemptsAndLegacyRecords(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().WithObjects(auditCM("legacy", "2026-01-01T00:00:00Z", "repo", "GO")).Build()
	for i, verdict := range []string{"INCOMPLETE", "GO"} {
		rec := audit.Record{
			SchemaVersion: audit.SchemaVersion, Attempt: int64(i + 1),
			Task: audit.TaskRef{Name: "retried", Namespace: "default", UID: "task-uid"}, Verdict: verdict,
		}
		if err := audit.WriteRecord(ctx, c, "default", rec, logr.Discard()); err != nil {
			t.Fatal(err)
		}
	}
	var out strings.Builder
	if err := exportAuditRecords(ctx, c, "default", "", "", &out); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("exported %d records, want 3: %s", len(lines), out.String())
	}
	if !strings.Contains(out.String(), `"attempt":1`) || !strings.Contains(out.String(), `"attempt":2`) ||
		!strings.Contains(out.String(), `"name":"legacy"`) {
		t.Fatalf("export lost attempt history: %s", out.String())
	}
}
