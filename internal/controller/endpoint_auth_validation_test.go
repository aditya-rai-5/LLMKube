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
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/rand"

	inferencev1alpha1 "github.com/defilantech/llmkube/api/v1alpha1"
)

// spec.endpoint.apiKeySecretRef and externalTrafficPolicy against the envtest
// apiserver: a failure means the generated CRD does not enforce the CEL rules.
var _ = Describe("InferenceService endpoint auth CRD validation", func() {
	ctx := context.Background()
	keyRef := &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "upstream-key"}, Key: "key",
	}
	newISvc := func(runtime string, ep *inferencev1alpha1.EndpointSpec) *inferencev1alpha1.InferenceService {
		return &inferencev1alpha1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "ep-auth-" + rand.String(6), Namespace: "default"},
			Spec: inferencev1alpha1.InferenceServiceSpec{
				ModelRef: "ep-auth-model", Runtime: runtime, Endpoint: ep,
			},
		}
	}
	admit := func(isvc *inferencev1alpha1.InferenceService) {
		Expect(k8sClient.Create(ctx, isvc)).To(Succeed())
		Expect(k8sClient.Delete(ctx, isvc)).To(Succeed())
	}
	reject := func(isvc *inferencev1alpha1.InferenceService, substr string) {
		err := k8sClient.Create(ctx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(substr))
	}

	for _, rt := range []string{"vllm", "sglang"} {
		It("admits apiKeySecretRef on "+rt, func() {
			admit(newISvc(rt, &inferencev1alpha1.EndpointSpec{APIKeySecretRef: keyRef}))
		})
	}
	for _, rt := range []string{"", "llamacpp", "tgi", "generic"} {
		It("rejects apiKeySecretRef on runtime "+rt, func() {
			reject(newISvc(rt, &inferencev1alpha1.EndpointSpec{APIKeySecretRef: keyRef}),
				"apiKeySecretRef is supported for runtime vllm and sglang")
		})
	}
	It("rejects apiKeySecretRef with spec.command on sglang", func() {
		isvc := newISvc("sglang", &inferencev1alpha1.EndpointSpec{APIKeySecretRef: keyRef})
		isvc.Spec.Command = []string{"python3", "-m", "sglang.launch_server"}
		reject(isvc, "LLMKube cannot configure SGLang endpoint authentication; "+
			"drop spec.command, or remove endpoint.apiKeySecretRef and configure authentication in your custom command")
	})
	It("admits apiKeySecretRef with spec.command on vllm (the key travels as env)", func() {
		isvc := newISvc("vllm", &inferencev1alpha1.EndpointSpec{APIKeySecretRef: keyRef})
		isvc.Spec.Command = []string{"vllm", "serve"}
		admit(isvc)
	})
	for _, typ := range []string{"NodePort", "LoadBalancer"} {
		It("admits externalTrafficPolicy Local with type "+typ, func() {
			admit(newISvc("vllm", &inferencev1alpha1.EndpointSpec{Type: typ, ExternalTrafficPolicy: "Local"}))
		})
	}
	It("rejects externalTrafficPolicy on the default ClusterIP type", func() {
		reject(newISvc("vllm", &inferencev1alpha1.EndpointSpec{ExternalTrafficPolicy: "Local"}),
			"externalTrafficPolicy requires endpoint.type NodePort or LoadBalancer")
	})
	It("rejects an unknown externalTrafficPolicy", func() {
		reject(newISvc("vllm", &inferencev1alpha1.EndpointSpec{Type: "NodePort", ExternalTrafficPolicy: "Bogus"}),
			"Unsupported value")
	})

	// The InferenceService gateway reconciler generates routes that carry no
	// credential, so a keyed backend behind it would answer 401 to every
	// request while GatewayReady reads True.
	gateway := func(enabled bool) *inferencev1alpha1.GatewaySpec {
		return &inferencev1alpha1.GatewaySpec{
			Enabled: enabled, GatewayRef: inferencev1alpha1.GatewayReference{Name: "ai-gateway"},
		}
	}
	It("rejects apiKeySecretRef with gateway.enabled", func() {
		reject(newISvc("vllm", &inferencev1alpha1.EndpointSpec{APIKeySecretRef: keyRef, Gateway: gateway(true)}),
			"endpoint.apiKeySecretRef cannot be combined with endpoint.gateway.enabled")
	})
	It("admits apiKeySecretRef with a gateway block that is not enabled", func() {
		admit(newISvc("vllm", &inferencev1alpha1.EndpointSpec{APIKeySecretRef: keyRef, Gateway: gateway(false)}))
	})
	It("admits gateway.enabled without apiKeySecretRef", func() {
		admit(newISvc("vllm", &inferencev1alpha1.EndpointSpec{Gateway: gateway(true)}))
	})

	// A nameless or malformed reference passes admission without these rules,
	// then the Deployment update is rejected and the old keyless pods keep
	// serving while the InferenceService reads Ready.
	const badRefMsg = "endpoint.apiKeySecretRef needs a non-empty Secret name and a valid Secret key"
	It("rejects apiKeySecretRef with no name", func() {
		reject(newISvc("vllm", &inferencev1alpha1.EndpointSpec{
			APIKeySecretRef: &corev1.SecretKeySelector{Key: "key"},
		}), badRefMsg)
	})
	It("rejects apiKeySecretRef with an explicitly empty name", func() {
		// The typed client drops name:"" (omitempty), so send it verbatim.
		u := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": inferencev1alpha1.GroupVersion.String(),
			"kind":       "InferenceService",
			"metadata":   map[string]any{"name": "ep-auth-" + rand.String(6), "namespace": "default"},
			"spec": map[string]any{
				"modelRef": "ep-auth-model", "runtime": "vllm",
				"endpoint": map[string]any{"apiKeySecretRef": map[string]any{"name": "", "key": "key"}},
			},
		}}
		err := k8sClient.Create(ctx, u)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(badRefMsg))
	})
	It("rejects apiKeySecretRef with a key that is not a valid Secret key", func() {
		reject(newISvc("vllm", &inferencev1alpha1.EndpointSpec{
			APIKeySecretRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "upstream-key"}, Key: "bad/key",
			},
		}), badRefMsg)
	})
	It("admits apiKeySecretRef with a valid name and a dotted key", func() {
		admit(newISvc("vllm", &inferencev1alpha1.EndpointSpec{
			APIKeySecretRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "upstream-key"}, Key: "api_key.v-2",
			},
		}))
	})
})

// spec.endpoint.apiKeySecretRef and externalTrafficPolicy are InferenceService
// fields; the router-proxy (internal/controller/router_service_builder.go
// newRouterService) only reads endpoint.port and endpoint.type, so a silently
// accepted apiKeySecretRef would make a user believe the router endpoint is
// authenticated when it is not. A failure here means the generated CRD does
// not enforce the CEL rule that blocks both fields on ModelRouter.
var _ = Describe("ModelRouter endpoint auth CRD validation", func() {
	ctx := context.Background()
	keyRef := &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "upstream-key"}, Key: "key",
	}
	newMR := func(ep *inferencev1alpha1.EndpointSpec) *inferencev1alpha1.ModelRouter {
		return &inferencev1alpha1.ModelRouter{
			ObjectMeta: metav1.ObjectMeta{Name: "mr-ep-auth-" + rand.String(6), Namespace: "default"},
			Spec: inferencev1alpha1.ModelRouterSpec{
				Backends: []inferencev1alpha1.RouterBackend{
					{Name: "primary", External: &inferencev1alpha1.ExternalProvider{Provider: "anthropic", Model: "claude-opus-4-7"}},
				},
				Endpoint: ep,
			},
		}
	}
	admit := func(mr *inferencev1alpha1.ModelRouter) {
		Expect(k8sClient.Create(ctx, mr)).To(Succeed())
		Expect(k8sClient.Delete(ctx, mr)).To(Succeed())
	}
	reject := func(mr *inferencev1alpha1.ModelRouter, substr string) {
		err := k8sClient.Create(ctx, mr)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(substr))
	}

	It("admits a minimal endpoint", func() {
		admit(newMR(&inferencev1alpha1.EndpointSpec{Port: 8080}))
	})
	It("rejects apiKeySecretRef", func() {
		reject(newMR(&inferencev1alpha1.EndpointSpec{APIKeySecretRef: keyRef}),
			"ModelRouter does not support them")
	})
	It("rejects externalTrafficPolicy", func() {
		reject(newMR(&inferencev1alpha1.EndpointSpec{Type: "NodePort", ExternalTrafficPolicy: "Local"}),
			"ModelRouter does not support them")
	})
})
