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

package agent_test

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	fake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	foremanv1alpha1 "github.com/defilantech/llmkube/api/foreman/v1alpha1"
	foremanagent "github.com/defilantech/llmkube/pkg/foreman/agent"
	"github.com/defilantech/llmkube/pkg/foreman/agent/oai"
	"github.com/defilantech/llmkube/pkg/foreman/agent/repo"
)

// These tests drive a REAL end-to-end coder pass (the same harness the envtest
// loop tests use) rather than calling Publish directly, because the thing under
// test is the wiring: that the executor actually hands loop turns to the
// stream. A unit test that publishes by hand would pass with the hook
// disconnected, which is the exact defect this guards.

// streamWiringExecutor is envtestLoopExecutor minus the envtest runner, plus a
// stream. It deliberately reuses the same fake OAI + fake registry harness so a
// change that breaks the loop's turn accounting fails here too.
func streamWiringExecutor(
	t *testing.T, root, bare, oaiURL string, stream *foremanagent.TurnStream,
) *foremanagent.NativeAgentLoopExecutor {
	t.Helper()
	agent, task := taskAndAgent("stream-wiring")
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(agent, task).Build()
	reg := &seqEnvtestRegistry{verdicts: []string{"GO"}}
	return &foremanagent.NativeAgentLoopExecutor{
		Client:                   c,
		WorkspaceRoot:            filepath.Join(root, "ws"),
		GitRemoteURL:             bare,
		UpstreamURLForRepo:       func(string) string { return bare },
		InferenceBaseURLOverride: oaiURL + "/v1",
		CommitAuthor:             repo.Identity{Name: "Foreman Bot", Email: "bot@foreman.test"},
		CommitCommitter:          repo.Identity{Name: "Foreman Bot", Email: "bot@foreman.test"},
		RegistryFactory: func(
			_ context.Context, ws string, _ *foremanv1alpha1.Agent, _ bool,
		) (foremanagent.ToolRegistry, error) {
			reg.workspace = ws
			return reg, nil
		},
		AuthFactory: fakeAuth(t),
		Stream:      stream,
	}
}

// A run on an executor holding a stream publishes that run's real turns.
func TestNativeExecutor_PublishesTurnsToStream(t *testing.T) {
	gitOrSkip(t)
	root := t.TempDir()
	bare := initBareWithSeed(t, root)
	oaiSrv := scriptedOAI(t, []string{submitGoBody})
	stream := foremanagent.NewTurnStream(16)

	e := streamWiringExecutor(t, root, bare, oaiSrv.URL, stream)
	_, task := taskAndAgent("stream-wiring")
	if _, err := execWithAgent(t, e, task); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	// Subscribe AFTER the run: the replay buffer is what makes a viewer that
	// attaches mid-run (or just after) still see what happened, so draining it
	// here exercises the same path the SSE handler uses.
	ch, cancel := stream.Subscribe()
	defer cancel()

	var events []foremanagent.TurnEvent
	for {
		select {
		case ev := <-ch:
			events = append(events, ev)
			continue
		default:
		}
		break
	}

	if len(events) == 0 {
		t.Fatal("stream received no turns; the executor never wired OnTurn to the stream")
	}

	// Turn 0 is the pre-loop system+user seed, not a turn the model took. It
	// leaking into the stream was a real bug in the flusher (fixed by seeding
	// emitted), so guard it here at the wired level too.
	var sawSubmit bool
	for i, ev := range events {
		if ev.Turn < 1 {
			t.Errorf("event %d has turn %d; turns are 1-based and the pre-loop seed must not publish", i, ev.Turn)
		}
		if len(ev.Messages) == 0 {
			t.Errorf("event %d (turn %d) carries no messages", i, ev.Turn)
		}
		for _, m := range ev.Messages {
			if m.Role != oai.RoleAssistant {
				continue
			}
			for _, tc := range m.ToolCalls {
				if tc.Function.Name == "submit_result" {
					sawSubmit = true
				}
			}
		}
	}

	// Proves the events carry the RUN's transcript and not empty scaffolding.
	if !sawSubmit {
		t.Errorf("no assistant submit_result tool call in %d published event(s); "+
			"the stream is not carrying the real transcript", len(events))
	}
}

// A nil stream must leave the loop untouched: the streaming path is opt-in and
// every existing deployment runs with it unset.
func TestNativeExecutor_NilStreamDoesNotPanic(t *testing.T) {
	gitOrSkip(t)
	root := t.TempDir()
	bare := initBareWithSeed(t, root)
	oaiSrv := scriptedOAI(t, []string{submitGoBody})

	e := streamWiringExecutor(t, root, bare, oaiSrv.URL, nil)
	_, task := taskAndAgent("stream-wiring")
	res, err := execWithAgent(t, e, task)
	if err != nil {
		t.Fatalf("Execute with nil stream: %v", err)
	}
	if res.Verdict != foremanv1alpha1.AgenticTaskVerdictGo {
		t.Fatalf("verdict with nil stream: want GO got %s", res.Verdict)
	}
}

// This must see progress before the upstream finishes, through the real
// executor wiring and HTTP handler, rather than publishing an event by hand.
func TestNativeExecutor_ProgressBeforeCompletion(t *testing.T) {
	gitOrSkip(t)
	root := t.TempDir()
	bare := initBareWithSeed(t, root)
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(`data: {"choices":[{"index":0,"delta":{"reasoning_content":"thinking"}}]}` + "\n\n"))
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write([]byte(`data: {"choices":[{"index":0,"delta":{"tool_calls":[` +
			`{"index":0,"id":"tc-1","type":"function","function":{"name":"submit_result",` +
			`"arguments":"{\"verdict\":\"GO\",\"summary\":\"fixed\"}"}}]},"finish_reason":"tool_calls"}]}` + "\n\n"))
	}))
	defer upstream.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream := foremanagent.NewTurnStream(4)
	server := httptest.NewServer(stream.Handler())
	defer server.Close()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	for stream.SubscriberCount() == 0 {
		select {
		case <-ctx.Done():
			t.Fatal("stream did not subscribe")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	e := streamWiringExecutor(t, root, bare, upstream.URL, stream)
	agent, task := taskAndAgent("stream-wiring")
	done := make(chan error, 1)
	go func() { _, err := e.Execute(ctx, task, agent); done <- err }()
	// Cancel before waiting for the HTTP servers during failure cleanup.
	defer cancel()
	reader := bufio.NewReader(resp.Body)
	name, payload, err := readStreamFrame(reader)
	if err != nil {
		t.Fatalf("no progress before completion: %v", err)
	}
	if name != "progress" {
		t.Fatalf("event = %q, want progress", name)
	}
	var progress struct {
		Turn      int  `json:"turn"`
		Tokens    int  `json:"tokens"`
		Attempt   int  `json:"attempt"`
		Estimated bool `json:"estimated"`
	}
	if err := json.Unmarshal([]byte(payload), &progress); err != nil {
		t.Fatal(err)
	}
	if progress.Turn != 1 || progress.Tokens != 1 || progress.Attempt != 1 || !progress.Estimated {
		t.Fatalf("unexpected progress: %+v", progress)
	}
	if strings.Contains(payload, "thinking") {
		t.Fatal("progress leaked reasoning text")
	}
	close(release)
	name, payload, err = readStreamFrame(reader)
	if err != nil {
		t.Fatal(err)
	}
	if name != "" || !strings.Contains(payload, "submit_result") {
		t.Fatalf("completed turn wire format changed: event=%q data=%s", name, payload)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func readStreamFrame(r *bufio.Reader) (name, data string, err error) {
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return "", "", err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" && data != "" {
			return name, data, nil
		}
		if v, ok := strings.CutPrefix(line, "event: "); ok {
			name = v
		}
		if v, ok := strings.CutPrefix(line, "data: "); ok {
			data = v
		}
	}
}
