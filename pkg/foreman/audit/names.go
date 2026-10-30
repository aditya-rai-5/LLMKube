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
	"fmt"
	"strings"
)

// AttemptKey distinguishes redispatches and delete-and-recreate retries alike.
func AttemptKey(uid string, attempt int64) string {
	return fmt.Sprintf("%s-attempt-%d", uid, attempt)
}

// ConfigMapName keeps the attempt suffix intact when a task has a long name.
func ConfigMapName(prefix, taskName, uid string, attempt int64) string {
	suffix := "-" + AttemptKey(uid, attempt)
	if uid == "" && attempt == 0 {
		// Legacy records without a UID have no reliable execution identity.
		suffix = ""
	}
	const maxName = 253
	if limit := maxName - len(prefix) - len(suffix); len(taskName) > limit {
		taskName = strings.TrimRight(taskName[:limit], "-.")
	}
	return prefix + taskName + suffix
}
