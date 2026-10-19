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
	"slices"

	corev1 "k8s.io/api/core/v1"

	inferencev1alpha1 "github.com/defilantech/llmkube/api/v1alpha1"
)

// Env var names the runtimes read the endpoint API key from. vLLM reads
// VLLM_API_KEY itself (v0.25.1 and v0.31.0: args.api_key or envs.VLLM_API_KEY);
// SGLang's launcher transfers LLMKUBE_API_KEY into a private config file.
const (
	vllmAPIKeyEnv   = "VLLM_API_KEY"    //nolint:gosec // G101: env var name, not a credential value
	sglangAPIKeyEnv = "LLMKUBE_API_KEY" //nolint:gosec // G101: env var name, not a credential value
)

// endpointAPIKeyRef returns the endpoint API key Secret reference LLMKube
// should wire, or nil when none is set or the user already passes --api-key in
// extraArgs (the user's flag wins; injecting a second key would be ambiguous).
func endpointAPIKeyRef(isvc *inferencev1alpha1.InferenceService) *corev1.SecretKeySelector {
	if isvc == nil || isvc.Spec.Endpoint == nil || isvc.Spec.Endpoint.APIKeySecretRef == nil {
		return nil
	}
	if hasMatchingExtraArg(isvc.Spec.ExtraArgs, "api-key") {
		return nil
	}
	return isvc.Spec.Endpoint.APIKeySecretRef
}

// inferenceServiceRequiresAPIKey reports whether the server answers 401 to a
// request without the key, in any form a user can set it: apiKeySecretRef,
// --api-key in extraArgs, --api-key in spec.args, which the native runtimes
// pass through only when spec.command is set, or a hand-set VLLM_API_KEY in
// spec.env. endpointAPIKeyRef is not enough: it returns nil when extraArgs
// already carries the flag.
func inferenceServiceRequiresAPIKey(isvc *inferencev1alpha1.InferenceService) bool {
	if isvc == nil {
		return false
	}
	if isvc.Spec.Endpoint != nil && isvc.Spec.Endpoint.APIKeySecretRef != nil {
		return true
	}
	if hasMatchingExtraArg(isvc.Spec.ExtraArgs, "api-key") {
		return true
	}
	if len(isvc.Spec.Command) > 0 && hasMatchingExtraArg(isvc.Spec.Args, "api-key") {
		return true
	}
	// The env check is vLLM-only because the default runtime is llama.cpp,
	// which ignores the variable. Presence is the verdict: the vLLM docs do
	// not pin what an empty value does, and resolving valueFrom would need
	// Secret reads the detector must not make.
	return isvc.Spec.Runtime == RuntimeVLLM &&
		slices.ContainsFunc(isvc.Spec.Env, func(e corev1.EnvVar) bool { return e.Name == vllmAPIKeyEnv })
}

// apiKeyEnvVar renders the key as a non-optional secretKeyRef: a missing Secret
// or key must fail the container rather than start the server unauthenticated.
func apiKeyEnvVar(name string, ref *corev1.SecretKeySelector) corev1.EnvVar {
	sel := ref.DeepCopy()
	sel.Optional = nil
	return corev1.EnvVar{Name: name, ValueFrom: &corev1.EnvVarSource{SecretKeyRef: sel}}
}
