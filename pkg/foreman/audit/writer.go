/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package audit

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	foremanv1alpha1 "github.com/defilantech/llmkube/api/foreman/v1alpha1"
)

const (
	// AuditLabel marks an audit-record ConfigMap; AuditTaskLabel carries the
	// task name for filtered discovery (`-l foreman.llmkube.dev/audit=true`).
	AuditLabel     = "foreman.llmkube.dev/audit"
	AuditTaskLabel = "foreman.llmkube.dev/audit-task"

	// auditDataKey is the ConfigMap data key holding the JSON Record.
	auditDataKey = "audit.json"

	auditNamePrefix = "foreman-audit-"

	// AuditedAnnotation guards against re-writing a record on every reconcile
	// of an already-terminal task (which would emit duplicate audit lines).
	AuditedAnnotation        = "foreman.llmkube.dev/audited"
	AuditedAttemptAnnotation = "foreman.llmkube.dev/audited-attempt"

	// TranscriptLabel marks durable transcripts managed by the audit reaper.
	TranscriptLabel = "foreman.llmkube.dev/retained-transcript"
)

// AuditConfigMapName returns the ConfigMap name for one execution record.
func AuditConfigMapName(rec Record) string {
	return ConfigMapName(auditNamePrefix, rec.Task.Name, rec.Task.UID, rec.Attempt)
}

// IsRecorded accepts the old boolean marker only for pre-counter executions.
func IsRecorded(task *foremanv1alpha1.AgenticTask) bool {
	if task.Annotations[AuditedAnnotation] != "true" {
		return false
	}
	key := task.Annotations[AuditedAttemptAnnotation]
	return key == AttemptKey(string(task.UID), task.Status.Attempt) ||
		(key == "" && task.Status.Attempt == 0)
}

// ResolveAgent returns the task's Agent, or nil when there is no agentRef or
// the object cannot be read. A missing Agent is not an error: the record is
// written without the agent block rather than lost.
func ResolveAgent(
	ctx context.Context,
	c client.Client,
	task *foremanv1alpha1.AgenticTask,
	log logr.Logger,
) *foremanv1alpha1.Agent {
	if task.Spec.AgentRef == nil || task.Spec.AgentRef.Name == "" {
		return nil
	}
	var a foremanv1alpha1.Agent
	key := client.ObjectKey{Namespace: task.Namespace, Name: task.Spec.AgentRef.Name}
	if err := c.Get(ctx, key, &a); err != nil {
		log.Info("audit: agent not resolvable; recording without agent block",
			"agent", task.Spec.AgentRef.Name, "err", err.Error())
		return nil
	}
	return &a
}

// WriteRecord upserts a durable, NON-owner-ref'd ConfigMap holding rec, plus
// emits the record as a single structured log line. The absence of an owner
// reference is deliberate: the record must outlive the AgenticTask so it
// remains a compliance trail after task garbage-collection. namespace is the
// audit namespace (caller passes the task namespace when unset upstream).
func WriteRecord(ctx context.Context, c client.Client, namespace string, rec Record, log logr.Logger) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("audit: marshal record: %w", err)
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      AuditConfigMapName(rec),
			Namespace: namespace,
			Labels: map[string]string{
				AuditLabel:                  "true",
				AuditTaskLabel:              rec.Task.Name,
				"app.kubernetes.io/part-of": "foreman",
			},
			// No OwnerReferences: survives task GC by design.
		},
		Data: map[string]string{auditDataKey: string(data)},
	}

	existing := &corev1.ConfigMap{}
	key := client.ObjectKey{Namespace: namespace, Name: cm.Name}
	switch getErr := c.Get(ctx, key, existing); {
	case apierrors.IsNotFound(getErr):
		if err := c.Create(ctx, cm); err != nil {
			return fmt.Errorf("audit: create configmap: %w", err)
		}
	case getErr == nil:
		existing.Data = cm.Data
		existing.Labels = cm.Labels
		if err := c.Update(ctx, existing); err != nil {
			return fmt.Errorf("audit: update configmap: %w", err)
		}
	default:
		return fmt.Errorf("audit: get configmap: %w", getErr)
	}

	// Structured audit stream line for SIEM/Loki ingestion. The durable
	// ConfigMap above is the primary record; this is the streaming copy.
	log.Info("foreman.audit",
		"task", rec.Task.Name, "namespace", rec.Task.Namespace,
		"attempt", rec.Attempt, "taskUID", rec.Task.UID,
		"verdict", rec.Verdict, "node", rec.AssignedNode,
		"repo", rec.Repo, "issue", rec.Issue, "schemaVersion", rec.SchemaVersion)
	return nil
}

// RecordTerminal writes the audit record for a terminal task exactly once.
// It is a no-op if this attempt is already recorded. auditNamespace
// is the namespace for the record; empty means the task's own namespace. The
// referenced Agent is fetched best-effort (a missing Agent yields a record
// with no agent block, not an error). On success it stamps AuditedAnnotation
// and the attempt identity so subsequent reconciles skip only this run.
func RecordTerminal(
	ctx context.Context,
	c client.Client,
	task *foremanv1alpha1.AgenticTask,
	auditNamespace string,
	log logr.Logger,
) error {
	if IsRecorded(task) {
		return nil
	}
	ns := auditNamespace
	if ns == "" {
		ns = task.Namespace
	}

	agent := ResolveAgent(ctx, c, task, log)

	rec := BuildRecord(task, agent)
	if err := WriteRecord(ctx, c, ns, rec, log); err != nil {
		return err
	}

	patch := client.MergeFromWithOptions(task.DeepCopy(), client.MergeFromWithOptimisticLock{})
	if task.Annotations == nil {
		task.Annotations = map[string]string{}
	}
	task.Annotations[AuditedAnnotation] = "true"
	task.Annotations[AuditedAttemptAnnotation] = AttemptKey(string(task.UID), task.Status.Attempt)
	if err := c.Patch(ctx, task, patch); err != nil {
		return fmt.Errorf("audit: set audited annotation: %w", err)
	}
	return nil
}
