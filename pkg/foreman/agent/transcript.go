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
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	foremanv1alpha1 "github.com/defilantech/llmkube/api/foreman/v1alpha1"
	"github.com/defilantech/llmkube/pkg/foreman/agent/oai"
	"github.com/defilantech/llmkube/pkg/foreman/audit"
)

// TranscriptSchemaVersion identifies the on-disk shape of the transcript
// payload. The Kubernetes apiserver caps a single ConfigMap at 1 MiB
// total (including metadata); we leave 16 KiB of headroom for that
// metadata and the truncation marker so a runaway transcript fails
// gracefully rather than returning a 413 from the apiserver.
const (
	TranscriptSchemaVersion = "foreman.transcript.v1"

	transcriptCapBytes = (1 << 20) - (16 << 10) // 1 MiB - 16 KiB

	// transcriptDataKey is the well-known key inside the ConfigMap's Data
	// map where the JSON transcript lives. A reader doing
	//   `kubectl get cm <name> -o jsonpath='{.data.transcript\.json}'`
	// gets exactly that payload.
	transcriptDataKey = "transcript.json"

	// transcriptMetaKey carries the small summary block (turn count +
	// schema version + truncated flag) for cheap inspection without
	// parsing the full transcript.
	transcriptMetaKey = "meta.json"
)

// TranscriptDoc is the JSON payload stored at data["transcript.json"].
type TranscriptDoc struct {
	SchemaVersion string        `json:"schemaVersion"`
	TurnCount     int           `json:"turnCount"`
	Truncated     bool          `json:"truncated,omitempty"`
	Messages      []oai.Message `json:"messages"`
}

// TranscriptMeta is the small block stored at data["meta.json"] for
// cheap inspection. Reading just this avoids parsing the full
// transcript when all a tool wants is "did this fit?".
type TranscriptMeta struct {
	SchemaVersion string `json:"schemaVersion"`
	TurnCount     int    `json:"turnCount"`
	Truncated     bool   `json:"truncated"`
	BytesStored   int    `json:"bytesStored"`
}

// transcriptName returns the ConfigMap name for a task. We prefix to
// keep transcript ConfigMaps trivially discoverable
// (`kubectl get cm -l foreman.llmkube.dev/transcript-of=<task>`).
func transcriptName(task *foremanv1alpha1.AgenticTask) string {
	return audit.ConfigMapName("foreman-transcript-", task.Name, string(task.UID), task.Status.Attempt)
}

// WriteTranscript creates or updates a durable ConfigMap for this execution
// attempt, retained by the audit reaper. Returns the LocalObject
// reference the executor surfaces in Result.Extra.
//
// Over-budget transcripts are truncated from the middle. The head
// (system prompt + first turns) and tail (last turns + the
// submit_result envelope, when present) are the high-signal parts;
// dropping the middle preserves the start-and-end context a reviewer
// or a downstream evaluator most needs to see.
func WriteTranscript(
	ctx context.Context,
	c client.Client,
	task *foremanv1alpha1.AgenticTask,
	loopResult *LoopResult,
) (corev1.ObjectReference, error) {
	if task == nil || loopResult == nil {
		return corev1.ObjectReference{}, fmt.Errorf("WriteTranscript: task and loopResult required")
	}

	doc := TranscriptDoc{
		SchemaVersion: TranscriptSchemaVersion,
		TurnCount:     loopResult.Turns,
		Messages:      loopResult.Transcript,
	}
	docBytes, err := json.Marshal(doc)
	if err != nil {
		return corev1.ObjectReference{}, fmt.Errorf("WriteTranscript: marshal: %w", err)
	}
	if len(docBytes) > transcriptCapBytes {
		// fits reports whether the document would still be under the cap
		// with this message set. Marshalling the real doc (rather than
		// summing message sizes) keeps the envelope, escaping and the
		// marker in the accounting, so the search cannot overshoot.
		fits := func(candidate []oai.Message) bool {
			probe := doc
			probe.Messages = candidate
			probe.Truncated = true
			b, err := json.Marshal(probe)
			return err == nil && len(b) <= transcriptCapBytes
		}
		doc.Messages = truncateMessages(doc.Messages, loopResult.Turns, fits)
		doc.Truncated = true
		docBytes, err = json.Marshal(doc)
		if err != nil {
			return corev1.ObjectReference{}, fmt.Errorf("WriteTranscript: marshal truncated: %w", err)
		}
		// Final guard: if even the truncated form is still over budget
		// (huge single messages), drop messages until it fits. Pathological
		// case; preserve at least the system prompt.
		for len(docBytes) > transcriptCapBytes && len(doc.Messages) > 1 {
			doc.Messages = append(doc.Messages[:1], doc.Messages[len(doc.Messages)-1])
			docBytes, _ = json.Marshal(doc)
		}
	}

	meta := TranscriptMeta{
		SchemaVersion: TranscriptSchemaVersion,
		TurnCount:     loopResult.Turns,
		Truncated:     doc.Truncated,
		BytesStored:   len(docBytes),
	}
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return corev1.ObjectReference{}, fmt.Errorf("WriteTranscript: marshal meta: %w", err)
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      transcriptName(task),
			Namespace: task.Namespace,
			Labels: map[string]string{
				"foreman.llmkube.dev/transcript-of": task.Name,
				"app.kubernetes.io/part-of":         "foreman",
				audit.TranscriptLabel:               "true",
			},
		},
		Data: map[string]string{
			transcriptDataKey: string(docBytes),
			transcriptMetaKey: string(metaBytes),
		},
	}

	// Feedback loops can update a transcript within one execution. A new
	// claim or task UID gets a different name and cannot overwrite it.
	existing := &corev1.ConfigMap{}
	key := client.ObjectKey{Namespace: cm.Namespace, Name: cm.Name}
	getErr := c.Get(ctx, key, existing)
	switch {
	case apierrors.IsNotFound(getErr):
		if err := c.Create(ctx, cm); err != nil {
			return corev1.ObjectReference{}, fmt.Errorf("WriteTranscript: create cm: %w", err)
		}
	case getErr == nil:
		existing.Data = cm.Data
		existing.Labels = cm.Labels
		// Task GC must not remove evidence before the retention period ends.
		existing.OwnerReferences = cm.OwnerReferences
		if err := c.Update(ctx, existing); err != nil {
			return corev1.ObjectReference{}, fmt.Errorf("WriteTranscript: update cm: %w", err)
		}
	default:
		return corev1.ObjectReference{}, fmt.Errorf("WriteTranscript: get cm: %w", getErr)
	}

	return corev1.ObjectReference{
		Kind:       "ConfigMap",
		APIVersion: "v1",
		Namespace:  cm.Namespace,
		Name:       cm.Name,
	}, nil
}

// truncateMessages keeps the head (system prompt + first user message), a
// marker naming how much was dropped, and the LARGEST tail that still fits
// under the caller's budget.
//
// It used to keep a fixed last-10 tail regardless of budget, so a transcript
// one byte over the cap was cut to 13 messages and stored ~1% of the space it
// was allowed (#1672). Only long runs exceed the cap and long runs are
// disproportionately the failures, so the fixed split gutted exactly the
// transcripts worth keeping.
//
// fits reports whether a candidate message set still fits; the caller supplies
// it so the real document envelope is what gets measured. minTailMessages is
// the floor: this never retains less than the old fixed split did, so a
// pathological transcript degrades to the previous behaviour rather than worse.
// The head-and-tail shape is deliberate -- the start ("what was the task") and
// the end ("how did it finish", where submit_result lives) are the high-signal
// parts, and the middle is what a reader can most afford to lose.
func truncateMessages(msgs []oai.Message, totalTurns int, fits func([]oai.Message) bool) []oai.Message {
	if len(msgs) <= 12 {
		// Nothing meaningful to drop.
		return msgs
	}
	const minTailMessages = 10
	head := msgs[:2] // system + first user prompt

	build := func(tailN int) []oai.Message {
		if tailN > len(msgs)-len(head) {
			tailN = len(msgs) - len(head)
		}
		tail := msgs[len(msgs)-tailN:]
		dropped := len(msgs) - len(head) - len(tail)
		if dropped < 0 {
			dropped = 0
		}
		marker := oai.Message{
			Role: oai.RoleSystem,
			Content: fmt.Sprintf(
				"[transcript truncated: %d middle messages dropped to fit "+
					"the 1 MiB ConfigMap budget; %d turns total]",
				dropped, totalTurns),
		}
		out := make([]oai.Message, 0, len(head)+1+len(tail))
		out = append(out, head...)
		out = append(out, marker)
		out = append(out, tail...)
		return out
	}

	// Binary search the largest tail that fits. Falling back to the floor
	// when even that is over budget preserves the old behaviour; the caller's
	// final guard handles the pathological single-huge-message case.
	best := build(minTailMessages)
	lo, hi := minTailMessages+1, len(msgs)-len(head)
	for lo <= hi {
		mid := lo + (hi-lo)/2
		candidate := build(mid)
		if fits(candidate) {
			best = candidate
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return best
}
