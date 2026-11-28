package k8s

import (
	"context"
	"slices"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/wrossmorrow/k8s-agents/pkg/hub"
	"github.com/wrossmorrow/k8s-agents/pkg/sessions"
)

func TestBuildSandboxPodTopologyAndIsolation(t *testing.T) {
	cfg := PodConfig{
		GatewaySecretName:      "agent-gateway-token",
		GitHubAppSecretName:    "interactive-github-app",
		GitHubAppConfigMapName: "interactive-github-app-config",
		ModelCatalogConfigMap:  "session-model-catalog",
	}.WithDefaults()

	spec := hub.SpawnSpec{
		SessionID:   "019dbbc7-1111-2222-3333-444455556666",
		SessionName: "fix-tests",
		SessionKind: sessions.KindInteractive,
		Repo:        "wrossmorrow/k8s-agents",
		Toolchain:   "golang",
		HubURL:      "http://session-hub.session-system.svc.cluster.local:8787",
		HubToken:    "scoped-secret",
	}

	pod := BuildSandboxPod(cfg, spec)

	if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		t.Fatal("AutomountServiceAccountToken must be false on sandbox pods")
	}
	if pod.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Fatalf("RestartPolicy = %q, want Never", pod.Spec.RestartPolicy)
	}
	if len(pod.Spec.InitContainers) != 2 {
		t.Fatalf("expected 2 native sidecars (egress-proxy + github-auth), got %d", len(pod.Spec.InitContainers))
	}
	if pod.Spec.InitContainers[0].Name != "egress-proxy" || pod.Spec.InitContainers[1].Name != "github-auth" {
		t.Fatalf("unexpected sidecars: %s, %s", pod.Spec.InitContainers[0].Name, pod.Spec.InitContainers[1].Name)
	}
	if len(pod.Spec.Containers) != 1 || pod.Spec.Containers[0].Image != "ghcr.io/wrossmorrow/k8s-agents-sandbox-golang:latest" {
		t.Fatalf("unexpected main container: %+v", pod.Spec.Containers)
	}

	// Verify analyst sessions omit github-auth sidecar and wire analyst AGENTS.md + read-only token.
	analystSpec := spec
	analystSpec.SessionKind = sessions.KindAnalyst
	analystSpec.AnalystToken = "an_test_tok"
	analystPod := BuildSandboxPod(cfg, analystSpec)
	if len(analystPod.Spec.InitContainers) != 1 || analystPod.Spec.InitContainers[0].Name != "egress-proxy" {
		t.Fatalf("analyst pod should only have egress-proxy sidecar, got %+v", analystPod.Spec.InitContainers)
	}
	var hasAnalystMD, hasAnalystTok bool
	for _, ev := range analystPod.Spec.Containers[0].Env {
		if ev.Name == "PI_AGENTS_MD" && ev.Value == "/opt/agents/analyst.md" {
			hasAnalystMD = true
		}
		if ev.Name == "SESSION_ANALYST_TOKEN" && ev.ValueFrom != nil && ev.ValueFrom.SecretKeyRef != nil && ev.ValueFrom.SecretKeyRef.Key == "analyst-token" {
			hasAnalystTok = true
		}
	}
	if !hasAnalystMD || !hasAnalystTok {
		t.Fatalf("analyst pod missing expected env vars (hasAnalystMD=%v, hasAnalystTok=%v)", hasAnalystMD, hasAnalystTok)
	}
}

func TestBuildSandboxNetworkPolicyBlocksMetadataAndLateralTraffic(t *testing.T) {
	np := BuildSandboxNetworkPolicy(PodConfig{}.WithDefaults())
	if len(np.Spec.Ingress) != 1 {
		t.Fatalf("expected 1 ingress rule (session-hub only), got %d", len(np.Spec.Ingress))
	}
	var blockedMetadata bool
	for _, rule := range np.Spec.Egress {
		for _, peer := range rule.To {
			if peer.IPBlock != nil && slices.Contains(peer.IPBlock.Except, "169.254.169.254/32") {
				blockedMetadata = true
			}
		}
	}
	if !blockedMetadata {
		t.Fatal("NetworkPolicy must explicitly exclude 169.254.169.254/32")
	}
}

func TestPodSpawnerLifecycleAndRediscovery(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cfg := PodConfig{Namespace: "interactive-sessions"}.WithDefaults()
	spawner := NewPodSpawner(cs, cfg, nil)
	spawner.skipHealthCheck = true

	spec := hub.SpawnSpec{
		SessionID:   "sess-k8s-1",
		SessionKind: sessions.KindInteractive,
		Toolchain:   "agent",
		HubURL:      "http://session-hub.session-system.svc.cluster.local:8787",
		HubToken:    "tok-123",
	}

	// Simulate kubelet assigning a PodIP and transitioning the Pod to Running shortly after creation.
	go func() {
		time.Sleep(50 * time.Millisecond)
		pod, err := cs.CoreV1().Pods(cfg.Namespace).Get(context.Background(), PodName(spec.SessionID), metav1.GetOptions{})
		if err != nil {
			return
		}
		pod.UID = types.UID("uid-pod-1")
		pod.Status.Phase = corev1.PodRunning
		pod.Status.PodIP = "10.42.0.15"
		_, _ = cs.CoreV1().Pods(cfg.Namespace).UpdateStatus(context.Background(), pod, metav1.UpdateOptions{})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ep, err := spawner.Spawn(ctx, spec)
	if err != nil {
		t.Fatalf("Spawn failed: %v", err)
	}
	if ep != "http://10.42.0.15:8787" {
		t.Fatalf("endpoint = %q, want http://10.42.0.15:8787", ep)
	}

	// Verify egress ConfigMap, NetworkPolicy, and token Secret were created.
	if _, err := cs.CoreV1().ConfigMaps(cfg.Namespace).Get(ctx, EgressConfigMapName, metav1.GetOptions{}); err != nil {
		t.Fatalf("egress configmap missing: %v", err)
	}
	if _, err := cs.NetworkingV1().NetworkPolicies(cfg.Namespace).Get(ctx, NetworkPolicyName, metav1.GetOptions{}); err != nil {
		t.Fatalf("networkpolicy missing: %v", err)
	}
	if _, err := cs.CoreV1().Secrets(cfg.Namespace).Get(ctx, TokenSecretName(spec.SessionID), metav1.GetOptions{}); err != nil {
		t.Fatalf("token secret missing: %v", err)
	}

	// Simulate session-hub restart with a fresh PodSpawner instance -> rediscovers running PodIP and Secret token.
	spawner2 := NewPodSpawner(cs, cfg, nil)
	if got, ok := spawner2.Endpoint(spec.SessionID); !ok || got != "http://10.42.0.15:8787" {
		t.Fatalf("rediscovered endpoint = (%q, %v), want (http://10.42.0.15:8787, true)", got, ok)
	}
	if tok, ok := spawner2.LookupToken(spec.SessionID); !ok || tok != "tok-123" {
		t.Fatalf("rediscovered token = (%q, %v), want (tok-123, true)", tok, ok)
	}

	// Stop deletes the Pod via the restarted spawner instance.
	if err := spawner2.Stop(ctx, spec.SessionID); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	if spawner2.Running(spec.SessionID) {
		t.Fatal("expected Running=false after Stop")
	}
}
