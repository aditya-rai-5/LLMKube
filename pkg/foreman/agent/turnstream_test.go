package agent

import (
	"testing"
	"time"

	"github.com/defilantech/llmkube/pkg/foreman/agent/oai"
)

// TestTurnStream_ReplayThenTail is the property that makes a live viewer
// usable: opening the page mid-run must show the whole run so far, then
// continue live. Without replay the viewer only works if you were already
// watching when the run started, which is never when you want it.
func TestTurnStream_ReplayThenTail(t *testing.T) {
	s := NewTurnStream(8)
	s.Publish(1, []oai.Message{{Role: oai.RoleAssistant, Content: "one"}})
	s.Publish(2, []oai.Message{{Role: oai.RoleAssistant, Content: "two"}})

	ch, cancel := s.Subscribe()
	defer cancel()

	for want := 1; want <= 2; want++ {
		select {
		case ev := <-ch:
			if ev.Turn != want {
				t.Fatalf("replay out of order: got turn %d want %d", ev.Turn, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("turn %d was never replayed to a late subscriber", want)
		}
	}

	s.Publish(3, []oai.Message{{Role: oai.RoleAssistant, Content: "three"}})
	select {
	case ev := <-ch:
		if ev.Turn != 3 {
			t.Fatalf("live tail: got turn %d want 3", ev.Turn)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no live event after the replay handover")
	}
}

// TestTurnStream_SlowSubscriberDoesNotBlockPublish pins the constraint that
// matters most: OnTurn runs on the loop's own goroutine, so a browser tab that
// stops reading must never stall a coder run.
func TestTurnStream_SlowSubscriberDoesNotBlockPublish(t *testing.T) {
	s := NewTurnStream(2)
	_, cancel := s.Subscribe() // deliberately never drained
	defer cancel()

	done := make(chan struct{})
	go func() {
		for i := 1; i <= 200; i++ {
			s.Publish(i, []oai.Message{{Role: oai.RoleAssistant, Content: "x"}})
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Publish blocked on a slow subscriber; a stalled tab would stall the coder")
	}
}

// TestTurnStream_RingBufferBounded: a long run must not grow memory without
// bound, and the recent turns are the useful ones.
func TestTurnStream_RingBufferBounded(t *testing.T) {
	s := NewTurnStream(3)
	for i := 1; i <= 10; i++ {
		s.Publish(i, []oai.Message{{Role: oai.RoleAssistant, Content: "x"}})
	}
	ch, cancel := s.Subscribe()
	defer cancel()

	var got []int
	for i := 0; i < 3; i++ {
		select {
		case ev := <-ch:
			got = append(got, ev.Turn)
		case <-time.After(2 * time.Second):
			t.Fatalf("replay stopped after %d events, want 3", len(got))
		}
	}
	if len(got) != 3 || got[0] != 8 || got[2] != 10 {
		t.Errorf("ring kept %v, want the last three turns (8,9,10)", got)
	}
	select {
	case ev := <-ch:
		t.Errorf("ring replayed more than its capacity: extra turn %d", ev.Turn)
	default:
	}
}

// TestTurnStream_CancelUnsubscribes: a disconnected viewer must not leak a
// subscriber slot for the life of the process.
func TestTurnStream_CancelUnsubscribes(t *testing.T) {
	s := NewTurnStream(4)
	_, cancel := s.Subscribe()
	if n := s.SubscriberCount(); n != 1 {
		t.Fatalf("subscriber count = %d, want 1", n)
	}
	cancel()
	if n := s.SubscriberCount(); n != 0 {
		t.Errorf("subscriber count after cancel = %d, want 0", n)
	}
	cancel() // must be idempotent; an HTTP handler may defer it and return early
}

// TestTurnStream_NilStreamPublishIsSafe: the executor may run without a stream
// configured, and PublishTurn is wired straight to LoopConfig.OnTurn.
func TestTurnStream_NilStreamPublishIsSafe(t *testing.T) {
	var s *TurnStream
	s.Publish(1, []oai.Message{{Role: oai.RoleAssistant, Content: "x"}})
}

// The nil case must return a nil hook, not a method value on a nil receiver.
// Both are safe to call, but only nil lets the loop's flusher skip the work,
// which is what keeps an unwatched run free. A method value would silently
// pass a "does it panic" test while costing every run a reslice per turn.
func TestTurnHook_NilStreamYieldsNilHook(t *testing.T) {
	if hook := (&NativeAgentLoopExecutor{}).turnHook(); hook != nil {
		t.Error("turnHook with no stream: want nil hook so the loop skips per-turn work, got non-nil")
	}
}

func TestTurnHook_StreamYieldsWorkingHook(t *testing.T) {
	s := NewTurnStream(4)
	hook := (&NativeAgentLoopExecutor{Stream: s}).turnHook()
	if hook == nil {
		t.Fatal("turnHook with a stream: want a hook, got nil")
	}

	ch, cancel := s.Subscribe()
	defer cancel()
	hook(7, []oai.Message{{Role: oai.RoleAssistant, Content: "hi"}})

	select {
	case ev := <-ch:
		if ev.Turn != 7 || len(ev.Messages) != 1 || ev.Messages[0].Content != "hi" {
			t.Errorf("hook delivered %+v; want turn 7 carrying one assistant message", ev)
		}
	default:
		t.Error("hook did not deliver the turn to a subscriber")
	}
}

func TestTurnStream_ProgressLiveOnlyAndBounded(t *testing.T) {
	s := NewTurnStream(1)
	s.Publish(1, []oai.Message{{Role: oai.RoleAssistant, Content: "done"}})
	s.PublishProgress(2, oai.Progress{Tokens: 1})
	sub, cancel := s.subscribe(true)
	defer cancel()
	if ev := <-sub.ch; ev.Turn != 1 {
		t.Fatalf("replayed turn=%d", ev.Turn)
	}
	select {
	case ev := <-sub.progress:
		t.Fatalf("replayed stale progress: %+v", ev)
	default:
	}
	turns, stop := s.Subscribe()
	defer stop()
	<-turns // replay
	done := make(chan struct{})
	go func() {
		for i := 1; i <= 1000; i++ {
			s.PublishProgress(2, oai.Progress{Tokens: i})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("progress blocked generation")
	}
	if ev := <-sub.progress; ev.Tokens != 1000 {
		t.Fatalf("latest progress=%+v", ev)
	}
	select {
	case ev := <-turns:
		t.Fatalf("turn-only subscriber received progress: %+v", ev)
	default:
	}
	// Progress neither evicts turns from replay nor fills their live queue.
	late, stopLate := s.Subscribe()
	defer stopLate()
	if ev := <-late; ev.Turn != 1 {
		t.Fatalf("progress evicted replay: %+v", ev)
	}
	s.PublishProgress(2, oai.Progress{Tokens: 1001})
	s.Publish(2, []oai.Message{{Role: oai.RoleAssistant, Content: "finished"}})
	if ev := <-sub.ch; ev.Turn != 2 {
		t.Fatalf("completed turn=%+v", ev)
	}
	select {
	case ev := <-sub.progress:
		t.Fatalf("stale progress after completion: %+v", ev)
	default:
	}
	cancel()
	cancel()
	s.PublishProgress(3, oai.Progress{Tokens: 1})
	if _, open := <-sub.progress; open {
		t.Fatal("progress channel left open after cancel")
	}
}

func TestProgressHook_NilStream(t *testing.T) {
	if hook := (&NativeAgentLoopExecutor{}).progressHook(); hook != nil {
		t.Fatal("nil stream enabled progress")
	}
	var s *TurnStream
	s.PublishProgress(1, oai.Progress{Tokens: 1})
}
