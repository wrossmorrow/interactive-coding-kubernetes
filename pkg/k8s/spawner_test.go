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

func TestBuildTokenSecretAndLookupAnalystToken(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cfg := PodConfig{Namespace: "interactive-sessions"}.WithDefaults()
	spawner := NewPodSpawner(cs, cfg, nil)
	ctx := context.Background()

	// Defense in depth: non-analyst sessions must never get analyst-token even if passed one.
	interactiveSec := BuildTokenSecret(cfg.Namespace, "sess-int-1", sessions.KindInteractive, "hub-tok-int", "spurious-analyst-tok")
	if _, ok := interactiveSec.Labels[LabelKind]; ok {
		t.Fatalf("interactive secret should not carry analyst label, got %q", interactiveSec.Labels[LabelKind])
	}
	if _, ok := interactiveSec.Data["analyst-token"]; ok {
		t.Fatal("interactive secret should not carry analyst-token")
	}

	analystTok := "an_sess-an-1.deadbeef"
	analystSec := BuildTokenSecret(cfg.Namespace, "sess-an-1", sessions.KindAnalyst, "hub-tok-an", analystTok)
	if analystSec.Labels[LabelKind] != string(sessions.KindAnalyst) {
		t.Fatalf("analyst secret label = %q, want %q", analystSec.Labels[LabelKind], sessions.KindAnalyst)
	}
	if got := string(analystSec.Data["analyst-token"]); got != analystTok {
		t.Fatalf("analyst secret Data[analyst-token] = %q, want %q", got, analystTok)
	}

	for _, sec := range []*corev1.Secret{interactiveSec, analystSec} {
		if _, err := cs.CoreV1().Secrets(cfg.Namespace).Create(ctx, sec, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create secret %s: %v", sec.Name, err)
		}
	}

	if got, ok := spawner.LookupAnalystToken("sess-an-1"); !ok || got != analystTok {
		t.Fatalf("LookupAnalystToken(sess-an-1) = (%q, %v), want (%q, true)", got, ok, analystTok)
	}
	if got, ok := spawner.LookupAnalystToken("sess-int-1"); ok || got != "" {
		t.Fatalf("LookupAnalystToken(sess-int-1) = (%q, %v), want (\"\", false)", got, ok)
	}
	if got, ok := spawner.LookupAnalystToken("sess-missing"); ok || got != "" {
		t.Fatalf("LookupAnalystToken(sess-missing) = (%q, %v), want (\"\", false)", got, ok)
	}
	if got, ok := spawner.LookupAnalystToken(""); ok || got != "" {
		t.Fatalf("LookupAnalystToken(\"\") = (%q, %v), want (\"\", false)", got, ok)
	}

	// Verify TokenManager integrates with LookupAnalystToken for post-restart verification.
	tm := hub.NewTokenManager()
	tm.SetAnalystLookup(spawner.LookupAnalystToken)
	if !tm.VerifyAnalyst(analystTok) {
		t.Fatal("TokenManager.VerifyAnalyst should accept valid analyst token via LookupAnalystToken")
	}
	if tm.VerifyAnalyst("an_sess-an-1.wrongsecret") {
		t.Fatal("TokenManager.VerifyAnalyst should reject wrong secret for valid analyst session")
	}
	if tm.VerifyAnalyst("hub-tok-int") {
		t.Fatal("TokenManager.VerifyAnalyst should reject interactive hub token")
	}
}

func TestBuildSandboxPodResourceSizes(t *testing.T) {
	cfg := PodConfig{}.WithDefaults()
	cases := []struct {
		name      string
		kind      sessions.Kind
		toolchain string
		size      string
		wantCPU   string
		wantMem   string
		wantDisk  string
	}{
		{"golang auto defaults to medium", sessions.KindInteractive, "golang", "", "2", "4Gi", "8Gi"},
		{"rust auto defaults to medium", sessions.KindInteractive, "rust", "", "2", "4Gi", "8Gi"},
		{"python auto defaults to small", sessions.KindInteractive, "python", "", "1", "2Gi", "4Gi"},
		{"analyst auto defaults to small even on golang", sessions.KindAnalyst, "golang", "", "1", "2Gi", "4Gi"},
		{"explicit small overrides golang", sessions.KindInteractive, "golang", sessions.SizeSmall, "1", "2Gi", "4Gi"},
		{"explicit large", sessions.KindInteractive, "golang", sessions.SizeLarge, "4", "8Gi", "16Gi"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pod := BuildSandboxPod(cfg, hub.SpawnSpec{
				SessionID:   "sess-size-1",
				SessionKind: tc.kind,
				Toolchain:   tc.toolchain,
				Size:        tc.size,
			})
			req := pod.Spec.Containers[0].Resources.Requests
			if got := req.Cpu().String(); got != tc.wantCPU {
				t.Fatalf("cpu = %q, want %q", got, tc.wantCPU)
			}
			if got := req.Memory().String(); got != tc.wantMem {
				t.Fatalf("memory = %q, want %q", got, tc.wantMem)
			}
			if got := req.StorageEphemeral().String(); got != tc.wantDisk {
				t.Fatalf("ephemeral-storage = %q, want %q", got, tc.wantDisk)
			}
		})
	}
}

func TestPodSpawnerHandlesTerminatedAgentWithLiveSidecar(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cfg := PodConfig{Namespace: "interactive-sessions"}.WithDefaults()
	spawner := NewPodSpawner(cs, cfg, nil)
	spawner.skipHealthCheck = true

	sessionID := "sess-sidecar-1"
	stalePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      PodName(sessionID),
			Namespace: cfg.Namespace,
			UID:       types.UID("uid-old"),
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.42.0.99",
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "agent",
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, Reason: "Completed"},
					},
				},
			},
		},
	}
	ctx := context.Background()
	if _, err := cs.CoreV1().Pods(cfg.Namespace).Create(ctx, stalePod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create stale pod: %v", err)
	}

	// PodRunning with Terminated agent must NOT be considered running.
	if spawner.Running(sessionID) {
		t.Fatal("Running() = true for pod whose agent container terminated, want false")
	}

	// WaitExit must return immediately when agent container is already Terminated.
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	spawner.WaitExit(waitCtx, sessionID)
	if waitCtx.Err() != nil {
		t.Fatalf("WaitExit timed out on PodRunning with Terminated agent: %v", waitCtx.Err())
	}

	// Spawn must delete the stale pod and create a fresh pod with a new PodIP.
	go func() {
		for range 30 {
			time.Sleep(25 * time.Millisecond)
			pod, err := cs.CoreV1().Pods(cfg.Namespace).Get(context.Background(), PodName(sessionID), metav1.GetOptions{})
			if err != nil || pod.UID == "uid-old" {
				continue
			}
			pod.UID = types.UID("uid-new")
			pod.Status.Phase = corev1.PodRunning
			pod.Status.PodIP = "10.42.0.100"
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{
				{Name: "agent", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
			}
			_, _ = cs.CoreV1().Pods(cfg.Namespace).UpdateStatus(context.Background(), pod, metav1.UpdateOptions{})
			return
		}
	}()

	spawnCtx, spawnCancel := context.WithTimeout(ctx, 5*time.Second)
	defer spawnCancel()
	ep, err := spawner.Spawn(spawnCtx, hub.SpawnSpec{
		SessionID:   sessionID,
		SessionKind: sessions.KindInteractive,
		Toolchain:   "golang",
		HubURL:      "http://session-hub.session-system.svc.cluster.local:8787",
		HubToken:    "tok-fresh",
	})
	if err != nil {
		t.Fatalf("Spawn replacing terminated-agent pod failed: %v", err)
	}
	if ep != "http://10.42.0.100:8787" {
		t.Fatalf("endpoint = %q, want http://10.42.0.100:8787", ep)
	}
}
