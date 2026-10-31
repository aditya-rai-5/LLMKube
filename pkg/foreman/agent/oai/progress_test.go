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

package oai

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

func TestProgress_ThrottleAndOutputKinds(t *testing.T) {
	var got []Progress
	p := progressReporter{attempt: 1, callback: func(v Progress) { got = append(got, v) }}
	start := time.Unix(100, 0)
	cases := []struct {
		delta   string
		elapsed time.Duration
		want    []int
	}{
		{`{"role":"assistant"}`, 0, nil},
		{`{"content":"hello"}`, 0, []int{1}},
		{`{"reasoning_content":"think"}`, time.Second, []int{1}},
		{`{"tool_calls":[{"index":0,"function":{"arguments":"{}"}}]}`, 1500 * time.Millisecond, []int{1}},
		{`{"tool_calls":[{"index":0,"function":{"name":"read_file"}}]}`, 2 * time.Second, []int{1, 4}},
		{`{}`, 4 * time.Second, []int{1, 4}},
		{`{"tool_calls":[{"index":0,"id":"call-1","type":"function"}]}`, 4 * time.Second, []int{1, 4}},
		{`{"content":"text","reasoning_content":"reason"}`, 4 * time.Second, []int{1, 4, 5}},
	}
	for _, tc := range cases {
		var chunk ChatChunk
		if err := json.Unmarshal([]byte(`{"choices":[{"index":0,"delta":`+tc.delta+`}]}`), &chunk); err != nil {
			t.Fatal(err)
		}
		p.observe(chunk, start.Add(tc.elapsed))
		counts := make([]int, 0, len(got))
		for _, event := range got {
			counts = append(counts, event.Tokens)
			if event.Attempt != 1 || !event.Estimated {
				t.Fatalf("unexpected progress: %+v", event)
			}
		}
		if !slices.Equal(counts, tc.want) {
			t.Fatalf("delta %s: counters %v, want %v", tc.delta, counts, tc.want)
		}
	}
}

func TestClient_ProgressRetryAndRequestReset(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		if calls.Add(1) == 1 {
			_, _ = w.Write([]byte(`data: {"choices":[{"index":0,"delta":{"tool_calls":[` +
				`{"index":0,"function":{"arguments":"{"}}]},"finish_reason":"tool_calls"}]}` + "\n\n"))
			return
		}
		_, _ = w.Write([]byte(`data: {"choices":[{"index":0,"delta":{"content":"ok"},"finish_reason":"stop"}]}` + "\n\n"))
	}))
	defer srv.Close()
	c := New(srv.URL, time.Second, 1, WithBackoffs(nil))
	var got []Progress
	callback := func(p Progress) { got = append(got, p) }
	for i := 0; i < 2; i++ {
		if _, err := c.ChatWithProgress(context.Background(), ChatRequest{}, callback); err != nil {
			t.Fatal(err)
		}
	}
	want := []Progress{
		{Tokens: 1, Attempt: 1, Estimated: true},
		{Tokens: 1, Attempt: 2, Estimated: true},
		{Tokens: 1, Attempt: 1, Estimated: true},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("progress = %+v, want %+v", got, want)
	}
}

func TestClient_ProgressCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(`data: {"choices":[{"index":0,"delta":{"content":"hello"}}]}` + "\n\n"))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	calls := 0
	_, err := New(srv.URL, time.Second, 1).ChatWithProgress(ctx, ChatRequest{}, func(Progress) { calls++; cancel() })
	if calls != 1 || !errors.Is(err, context.Canceled) {
		t.Fatalf("callbacks=%d error=%v", calls, err)
	}
}
