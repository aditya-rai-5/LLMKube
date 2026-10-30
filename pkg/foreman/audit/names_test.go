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

package audit

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/validation"
)

func TestConfigMapNamePreservesAttemptSuffix(t *testing.T) {
	for _, prefix := range []string{"foreman-audit-", "foreman-transcript-"} {
		name := ConfigMapName(prefix, strings.Repeat("x", 253), "12345678-1234-1234-1234-123456789abc", 2)
		if errors := validation.IsDNS1123Subdomain(name); len(errors) != 0 {
			t.Fatalf("invalid name %q: %v", name, errors)
		}
		if !strings.HasSuffix(name, "-12345678-1234-1234-1234-123456789abc-attempt-2") {
			t.Fatalf("lost unique suffix: %s", name)
		}
	}
}
