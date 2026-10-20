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

package controller

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	inferencev1alpha1 "github.com/defilantech/llmkube/api/v1alpha1"
)

const (
	sglangAPIKeyVolume     = "sglang-endpoint-key"       //nolint:gosec // G101: volume name, not a credential value
	sglangAPIKeyDir        = "/run/llmkube-endpoint-key" //nolint:gosec // G101: mount path, not a credential value
	sglangAPIKeyConfigPath = sglangAPIKeyDir + "/config.yaml"
)

// JSON is valid YAML and quotes arbitrary Secret bytes without interpolation.
// SGLang's --config parser supports this in the pinned v0.5.15.post1 and
// v0.5.22. Merge a user's config because SGLang accepts only one config file;
// explicit CLI flags keep their precedence. This does not redact SGLang logs.
//
//nolint:gosec // G101: launcher source reads a Secret-backed env var; contains no credential value.
const sglangAPIKeyLauncher = `import json, os, sys
path = sys.argv[1]
args = sys.argv[2:]
config = {}
user_configs = []
remaining = []
i = 0
while i < len(args):
    arg = args[i]
    if arg == "--config":
        if i + 1 == len(args):
            raise SystemExit("missing SGLang config path")
        value = args[i + 1]
        i += 2
    elif arg.startswith("--config="):
        value = arg.split("=", 1)[1]
        i += 1
    else:
        remaining.append(arg)
        i += 1
        continue
    if value != path:
        user_configs.append(value)
if len(user_configs) > 1:
    raise SystemExit("multiple SGLang config files are not supported")
if user_configs:
    import yaml
    with open(user_configs[0]) as source:
        config = yaml.safe_load(source) or {}
    if not isinstance(config, dict):
        raise SystemExit("SGLang config must contain a mapping")
key = os.environ.pop("LLMKUBE_API_KEY")
if not key:
    raise SystemExit("SGLang endpoint API key must not be empty")
config.pop("api_key", None)
config["api-key"] = key
os.umask(0o077)
with open(path, "w") as target:
    json.dump(config, target, default=str)
os.execv(sys.executable, [sys.executable, "-m", "sglang.launch_server", "--config", path] + remaining)
`

func configureSGLangAPIKey(deployment *appsv1.Deployment, isvc *inferencev1alpha1.InferenceService) {
	if isvc.Spec.Runtime != RuntimeSGLANG || len(isvc.Spec.Command) > 0 || endpointAPIKeyRef(isvc) == nil {
		return
	}
	pod := &deployment.Spec.Template.Spec
	container := &pod.Containers[0]
	container.Command = []string{"python3", "-c", sglangAPIKeyLauncher, sglangAPIKeyConfigPath}
	container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
		Name: sglangAPIKeyVolume, MountPath: sglangAPIKeyDir,
	})
	// A dedicated tmpfs works with readOnlyRootFilesystem and keeps the key
	// off the container's writable layer. No other container mounts it.
	limit := resource.MustParse("1Mi")
	pod.Volumes = append(pod.Volumes, corev1.Volume{
		Name: sglangAPIKeyVolume,
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{
			Medium: corev1.StorageMediumMemory, SizeLimit: &limit,
		}},
	})
}
