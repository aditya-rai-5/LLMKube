package agent

import (
	"sync"
	"time"

	"github.com/defilantech/llmkube/pkg/foreman/agent/oai"
)

// TurnEvent is one completed turn, as it happened.
type TurnEvent struct {
	// Turn is the 1-based turn number this event completes.
	Turn int `json:"turn"`
	// Messages are the transcript entries appended during this turn: the
	// assistant message (including its tool_calls, which is what makes the
	// stream show WHAT the agent wrote rather than merely that it wrote) and
	// the tool results that followed.
	Messages []oai.Message `json:"messages"`
	// At is when the turn completed, so a viewer can show pacing. Decode at
	// ~14 tok/s on a 397B means minutes between turns; without timestamps a
	// stalled run and a slow one look identical.
	At time.Time `json:"at"`
}

// TurnStream fans completed turns out to live subscribers and keeps a bounded
// replay buffer so a subscriber that joins mid-run sees what it missed.
//
// It deliberately does NOT persist anything. The transcript ConfigMap remains
// the archive, written once at completion. Conflating the two is what forces
// today's truncation compromise: transcripts are capped at the 1 MiB ConfigMap
// budget and silently drop middle messages once a run gets long.
//
// A nil *TurnStream is usable: Publish is a no-op. That keeps the executor
// wiring unconditional even when streaming is disabled.
type TurnStream struct {
	mu sync.Mutex
	// ring holds the most recent events, oldest first, capped at capacity.
	ring     []TurnEvent
	capacity int
	subs     map[*subscriber]struct{}
}

// ProgressEvent is live-only; it never occupies completed-turn replay capacity.
type ProgressEvent struct {
	Turn int `json:"turn"`
	oai.Progress
	At time.Time `json:"at"`
}

type subscriber struct {
	progress chan ProgressEvent
	ch       chan TurnEvent
	once     sync.Once
}

// NewTurnStream returns a stream retaining the last capacity turns for replay.
func NewTurnStream(capacity int) *TurnStream {
	if capacity < 1 {
		capacity = 1
	}
	return &TurnStream{
		ring:     make([]TurnEvent, 0, capacity),
		capacity: capacity,
		subs:     make(map[*subscriber]struct{}),
	}
}

// Publish records a completed turn and delivers it to current subscribers.
//
// NEVER BLOCKS. It runs on the loop's own goroutine via LoopConfig.OnTurn, so
// blocking here would stall the coder run itself. A subscriber whose buffer is
// full misses the event rather than holding up the agent; the replay buffer
// means a viewer that reconnects still catches up.
func (s *TurnStream) Publish(turn int, msgs []oai.Message) {
	if s == nil {
		return
	}
	// Copy: the loop hands us a slice of its live transcript, which it keeps
	// appending to. Retaining that slice would alias a growing backing array.
	cp := make([]oai.Message, len(msgs))
	copy(cp, msgs)
	ev := TurnEvent{Turn: turn, Messages: cp, At: time.Now()}

	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.ring) == s.capacity {
		s.ring = append(s.ring[:0], s.ring[1:]...)
	}
	s.ring = append(s.ring, ev)

	for sub := range s.subs {
		// A completed turn supersedes its pending progress.
		select {
		case <-sub.progress:
		default:
		}
		select {
		case sub.ch <- ev:
		default: // slow consumer; drop rather than stall the loop
		}
	}
}

// PublishTurn matches the LoopConfig.OnTurn signature so it can be assigned
// directly, including from a nil *TurnStream.
func (s *TurnStream) PublishTurn(turn int, msgs []oai.Message) {
	s.Publish(turn, msgs)
}

// Subscribe returns a channel delivering the replay buffer followed by live
// turns, and a cancel func that unsubscribes. cancel is idempotent, so an HTTP
// handler can defer it and also return early.
//
// The replay is loaded into the channel buffer under the same lock that
// registers the subscriber, so no turn is missed or duplicated across the
// replay-to-live handover.
func (s *TurnStream) Subscribe() (<-chan TurnEvent, func()) {
	sub, cancel := s.subscribe(false)
	return sub.ch, cancel
}

func (s *TurnStream) subscribe(withProgress bool) (*subscriber, func()) {
	if s == nil {
		ch := make(chan TurnEvent)
		close(ch)
		return &subscriber{ch: ch}, func() {}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Buffer the backlog plus headroom for live turns arriving while the
	// consumer works through it.
	sub := &subscriber{ch: make(chan TurnEvent, len(s.ring)+s.capacity)}
	if withProgress {
		sub.progress = make(chan ProgressEvent, 1)
	}
	for _, ev := range s.ring {
		sub.ch <- ev
	}
	s.subs[sub] = struct{}{}

	return sub, func() {
		sub.once.Do(func() {
			s.mu.Lock()
			delete(s.subs, sub)
			s.mu.Unlock()
			close(sub.ch)
			if sub.progress != nil {
				close(sub.progress)
			}
		})
	}
}

// SubscriberCount reports the number of live subscribers.
func (s *TurnStream) SubscriberCount() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.subs)
}

// turnHook returns the LoopConfig.OnTurn hook for this executor's stream, or
// nil when no stream is attached.
//
// The nil case must stay NIL rather than a method value on a nil *TurnStream.
// PublishTurn is safe to call on a nil receiver, but a method value is still a
// non-nil func, so returning one unconditionally would make the loop's flusher
// reslice the transcript on every turn of every run nobody is watching. The
// loop checks this hook against nil precisely so an unwatched run pays nothing.
func (e *NativeAgentLoopExecutor) turnHook() func(int, []oai.Message) {
	if e.Stream == nil {
		return nil
	}
	return e.Stream.PublishTurn
}

// PublishProgress keeps only the latest pending progress for each live viewer,
// independently of completed turns. A slow viewer cannot stall generation.
func (s *TurnStream) PublishProgress(turn int, progress oai.Progress) {
	if s == nil {
		return
	}
	ev := ProgressEvent{Turn: turn, Progress: progress, At: time.Now()}
	s.mu.Lock()
	defer s.mu.Unlock()
	for sub := range s.subs {
		if sub.progress == nil {
			continue
		}
		select {
		case <-sub.progress:
		default:
		}
		// Publishers hold mu, so the drained buffer always has a free slot.
		sub.progress <- ev
	}
}

func (e *NativeAgentLoopExecutor) progressHook() func(int, oai.Progress) {
	if e.Stream == nil {
		return nil
	}
	return e.Stream.PublishProgress
}
