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

import "time"

// Progress counts output-bearing deltas, an estimate of generated tokens:
// compatible servers may batch multiple tokens into one delta. No content is
// exposed. Attempt is 1-based; Tokens resets when the client retries.
type Progress struct {
	Tokens    int  `json:"tokens"`
	Attempt   int  `json:"attempt"`
	Estimated bool `json:"estimated"`
}

const progressInterval = 2 * time.Second

type progressReporter struct {
	callback func(Progress)
	attempt  int
	tokens   int
	last     time.Time
}

func (p *progressReporter) observe(chunk ChatChunk, now time.Time) {
	added := 0
	for _, choice := range chunk.Choices {
		if hasOutput(choice.Delta) {
			added++
		}
	}
	if added == 0 {
		return
	}
	p.tokens += added
	if !p.last.IsZero() && now.Sub(p.last) < progressInterval {
		return
	}
	p.last = now
	p.callback(Progress{Tokens: p.tokens, Attempt: p.attempt, Estimated: true})
}

func hasOutput(delta MessageDelta) bool {
	if delta.Content != "" || delta.ReasoningContent != "" {
		return true
	}
	for _, tc := range delta.ToolCalls {
		if tc.Function.Name != "" || tc.Function.Arguments != "" {
			return true
		}
	}
	return false
}
