package k8s

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	fstesting "k8s.io/client-go/testing"

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

func TestAgentContainerTerminatedInRunningPod(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cfg := PodConfig{Namespace: "interactive-sessions"}.WithDefaults()
	spawner := NewPodSpawner(cs, cfg, nil)
	spawner.skipHealthCheck = true

	spec := hub.SpawnSpec{
		SessionID:   "sess-agent-exit",
		SessionKind: sessions.KindInteractive,
		Toolchain:   "agent",
		HubURL:      "http://session-hub.session-system.svc.cluster.local:8787",
		HubToken:    "tok-agent-exit",
	}
	podName := PodName(spec.SessionID)

	// PodRunning pod with an IP, but the agent container has terminated while
	// a native sidecar keeps the pod phase Running.
	terminated := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: cfg.Namespace},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.42.0.99",
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "agent", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}},
				{Name: "egress-proxy", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
			},
		},
	}
	if _, err := cs.CoreV1().Pods(cfg.Namespace).Create(context.Background(), terminated, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create terminated pod: %v", err)
	}

	// A stale cached endpoint must be invalidated, not blindly returned.
	spawner.setEndpoint(spec.SessionID, "http://10.42.0.99:8787")
	if ep, ok := spawner.Endpoint(spec.SessionID); ok {
		t.Fatalf("Endpoint with stale cache = (%q, true), want (\"\", false)", ep)
	}

	// Endpoint/Running must treat the pod as not running even though phase is Running.
	if ep, ok := spawner.Endpoint(spec.SessionID); ok {
		t.Fatalf("Endpoint = (%q, true), want (\"\", false) for terminated agent container", ep)
	}
	if spawner.Running(spec.SessionID) {
		t.Fatal("Running = true, want false for terminated agent container")
	}

	// WaitExit must return promptly for a terminated agent container.
	weCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	done := make(chan struct{})
	go func() {
		spawner.WaitExit(weCtx, spec.SessionID)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("WaitExit did not return for terminated agent container")
	}
	cancel()

	// Simulate kubelet bringing the replacement pod to Running with a new IP.
	go func() {
		for i := 0; i < 100; i++ {
			time.Sleep(50 * time.Millisecond)
			pod, err := cs.CoreV1().Pods(cfg.Namespace).Get(context.Background(), podName, metav1.GetOptions{})
			if err != nil || pod.Status.Phase != "" {
				continue
			}
			pod.Status.Phase = corev1.PodRunning
			pod.Status.PodIP = "10.42.0.100"
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{
				{Name: "agent", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
			}
			_, _ = cs.CoreV1().Pods(cfg.Namespace).UpdateStatus(context.Background(), pod, metav1.UpdateOptions{})
		}
	}()

	// Spawn must delete the terminated pod and create a fresh one.
	ctx, cancel2 := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel2()
	ep, err := spawner.Spawn(ctx, spec)
	if err != nil {
		t.Fatalf("Spawn failed: %v", err)
	}
	if ep != "http://10.42.0.100:8787" {
		t.Fatalf("endpoint = %q, want http://10.42.0.100:8787", ep)
	}

	// Verify the old terminated pod was replaced by a fresh one.
	got, err := cs.CoreV1().Pods(cfg.Namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get replacement pod: %v", err)
	}
	if got.Status.PodIP != "10.42.0.100" {
		t.Fatalf("replacement pod IP = %q, want 10.42.0.100", got.Status.PodIP)
	}
	for _, cst := range got.Status.ContainerStatuses {
		if cst.Name == "agent" && cst.State.Terminated != nil {
			t.Fatal("agent container still terminated after Spawn replacement")
		}
	}
	if !spawner.Running(spec.SessionID) {
		t.Fatal("expected Running=true after respawn")
	}
}

func TestWatchExitIgnoresReplacedPodGeneration(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cfg := PodConfig{Namespace: "interactive-sessions"}.WithDefaults()
	exitCh := make(chan string, 1)
	spawner := NewPodSpawner(cs, cfg, func(id string) { exitCh <- id })

	spec := hub.SpawnSpec{SessionID: "sess-replaced-watcher"}
	podName := PodName(spec.SessionID)

	// The pod now occupying the name is a different generation (new UID)
	// whose agent has already terminated.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: cfg.Namespace, UID: types.UID("uid-new-gen")},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.42.0.50",
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "agent", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}},
			},
		},
	}
	if _, err := cs.CoreV1().Pods(cfg.Namespace).Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create replaced pod: %v", err)
	}

	// The stale watcher (started for the old generation) must stop silently
	// instead of firing onExit for the new generation.
	go spawner.watchExit(spec.SessionID, podName, types.UID("uid-old-gen"))
	select {
	case id := <-exitCh:
		t.Fatalf("onExit fired for replaced pod generation: %s", id)
	case <-time.After(4 * time.Second):
	}
}

func TestWatchExitStaysSilentDuringInFlightReplacement(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cfg := PodConfig{Namespace: "interactive-sessions"}.WithDefaults()
	exitCh := make(chan string, 1)
	spawner := NewPodSpawner(cs, cfg, func(id string) { exitCh <- id })
	spawner.exitPoll = 50 * time.Millisecond

	sessionID := "sess-replace-gap"
	podName := PodName(sessionID)
	// No pod exists: the old generation's watcher is in the delete->create
	// gap of a replacement that Spawn is currently performing.
	spawner.markReplacing(sessionID)
	defer spawner.unmarkReplacing(sessionID)
	go spawner.watchExit(sessionID, podName, types.UID("uid-old-gen"))

	// Many more than two ticks elapse while the replacement is in flight;
	// the stale watcher must stay silent even though the pod is missing.
	select {
	case id := <-exitCh:
		t.Fatalf("onExit fired while replacement in flight: %s", id)
	case <-time.After(1 * time.Second):
	}

	// Once the replacement mark is cleared (spawn failed / returned), the
	// watcher must report the exit on the next debounced ticks.
	spawner.unmarkReplacing(sessionID)
	select {
	case id := <-exitCh:
		if id != sessionID {
			t.Fatalf("onExit = %s, want %s", id, sessionID)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("onExit did not fire after replacement mark cleared")
	}
}

func TestSpawnFailsClearlyWhenStalePodSurvivesForceDelete(t *testing.T) {
	cs := fake.NewSimpleClientset()
	// Simulate a stuck deletion (finalizer / slow sidecar teardown): pod
	// Delete calls are acknowledged but the pod object stays behind.
	cs.PrependReactor("delete", "pods", func(action fstesting.Action) (bool, runtime.Object, error) {
		return true, nil, nil
	})
	cfg := PodConfig{Namespace: "interactive-sessions"}.WithDefaults()
	spawner := NewPodSpawner(cs, cfg, nil)
	spawner.skipHealthCheck = true
	spawner.deleteWait = 300 * time.Millisecond

	spec := hub.SpawnSpec{
		SessionID:   "sess-stuck-delete",
		SessionKind: sessions.KindInteractive,
		Toolchain:   "golang",
		HubURL:      "http://session-hub.session-system.svc.cluster.local:8787",
		HubToken:    "tok-stuck",
	}
	stalePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: PodName(spec.SessionID), Namespace: cfg.Namespace, UID: types.UID("uid-stuck")},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.42.0.77",
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "agent", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}},
			},
		},
	}
	if _, err := cs.CoreV1().Pods(cfg.Namespace).Create(context.Background(), stalePod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create stale pod: %v", err)
	}

	_, err := spawner.Spawn(context.Background(), spec)
	if err == nil {
		t.Fatal("Spawn succeeded, want a clear error while the stale pod still holds the name")
	}
	if !strings.Contains(err.Error(), "still exists after force-delete") {
		t.Fatalf("error = %q, want a clear force-delete message", err)
	}
	if spawner.isReplacing(spec.SessionID) {
		t.Fatal("replacement mark leaked after failed Spawn")
	}
}

func TestSpawnAdoptsConcurrentReplacementPod(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cfg := PodConfig{Namespace: "interactive-sessions"}.WithDefaults()
	tracker := cs.Tracker()
	// Deterministic stand-in for an overlapping Spawn (no per-session spawn
	// lock): as soon as the stale pod is force-deleted, a new-generation
	// pod (new UID, live agent) takes the same name.
	cs.PrependReactor("delete", "pods", func(action fstesting.Action) (bool, runtime.Object, error) {
		del := action.(fstesting.DeleteAction)
		podsGVR := corev1.SchemeGroupVersion.WithResource("pods")
		_ = tracker.Delete(podsGVR, cfg.Namespace, del.GetName())
		concurrent := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: del.GetName(), Namespace: cfg.Namespace, UID: types.UID("uid-new-gen")},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				PodIP: "10.42.0.200",
				ContainerStatuses: []corev1.ContainerStatus{
					{Name: "agent", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
				},
			},
		}
		_ = tracker.Add(concurrent)
		return true, nil, nil
	})
	spawner := NewPodSpawner(cs, cfg, nil)
	spawner.skipHealthCheck = true
	spawner.deleteWait = 500 * time.Millisecond

	spec := hub.SpawnSpec{
		SessionID:   "sess-concurrent-replace",
		SessionKind: sessions.KindInteractive,
		Toolchain:   "golang",
		HubURL:      "http://session-hub.session-system.svc.cluster.local:8787",
		HubToken:    "tok-concurrent",
	}
	stalePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: PodName(spec.SessionID), Namespace: cfg.Namespace, UID: types.UID("uid-old")},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.42.0.77",
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "agent", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}},
			},
		},
	}
	if _, err := cs.CoreV1().Pods(cfg.Namespace).Create(context.Background(), stalePod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create stale pod: %v", err)
	}

	// The concurrent replacement pod occupies the name while the stale pod
	// is being deleted; waitDeleted must return early on the foreign UID
	// and Spawn must adopt the replacement instead of failing with a
	// stale-pod error.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ep, err := spawner.Spawn(ctx, spec)
	if err != nil {
		t.Fatalf("Spawn with concurrent replacement pod failed: %v", err)
	}
	if ep != "http://10.42.0.200:8787" {
		t.Fatalf("endpoint = %q, want adopted http://10.42.0.200:8787", ep)
	}
	// The concurrent-generation pod must survive, not be replaced again.
	got, err := cs.CoreV1().Pods(cfg.Namespace).Get(ctx, PodName(spec.SessionID), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get replacement pod: %v", err)
	}
	if got.UID != "uid-new-gen" {
		t.Fatalf("pod UID = %q, want adopted uid-new-gen", got.UID)
	}
	if spawner.isReplacing(spec.SessionID) {
		t.Fatal("replacement mark leaked after adopting concurrent pod")
	}
}

func TestSpawnFailsClearlyWhenDeletionCannotBeConfirmed(t *testing.T) {
	cs := fake.NewSimpleClientset()
	// Deletions are acked but do not remove the pod, and every pod Get after
	// the preflight and existing-pod lookups fails with a transient API
	// error: waitDeleted polls into the deadline, and the confirmation
	// re-Get must surface the API error instead of falling through to a
	// Create that could hit AlreadyExists.
	cs.PrependReactor("delete", "pods", func(action fstesting.Action) (bool, runtime.Object, error) {
		return true, nil, nil
	})
	gets := 0
	cs.PrependReactor("get", "pods", func(action fstesting.Action) (bool, runtime.Object, error) {
		gets++
		if gets > 2 {
			return true, nil, fmt.Errorf("api server unavailable")
		}
		return false, nil, nil
	})
	cfg := PodConfig{Namespace: "interactive-sessions"}.WithDefaults()
	spawner := NewPodSpawner(cs, cfg, nil)
	spawner.skipHealthCheck = true
	spawner.deleteWait = 300 * time.Millisecond

	spec := hub.SpawnSpec{
		SessionID:   "sess-unconfirmed-delete",
		SessionKind: sessions.KindInteractive,
		Toolchain:   "golang",
		HubURL:      "http://session-hub.session-system.svc.cluster.local:8787",
		HubToken:    "tok-unconfirmed",
	}
	stalePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: PodName(spec.SessionID), Namespace: cfg.Namespace, UID: types.UID("uid-stuck")},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.42.0.77",
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "agent", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}},
			},
		},
	}
	if _, err := cs.CoreV1().Pods(cfg.Namespace).Create(context.Background(), stalePod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create stale pod: %v", err)
	}

	_, err := spawner.Spawn(context.Background(), spec)
	if err == nil {
		t.Fatal("Spawn succeeded, want a clear error when the deletion cannot be confirmed")
	}
	if !strings.Contains(err.Error(), "confirm deletion of stale sandbox pod") {
		t.Fatalf("error = %q, want a clear deletion-confirmation message", err)
	}
	if spawner.isReplacing(spec.SessionID) {
		t.Fatal("replacement mark leaked after failed Spawn")
	}
}

func TestCompareAndClearEndpointKeepsFreshEntry(t *testing.T) {
	spawner := NewPodSpawner(fake.NewSimpleClientset(), PodConfig{Namespace: "interactive-sessions"}.WithDefaults(), nil)

	// A stale cached value matching the current entry is cleared...
	spawner.setEndpoint("s1", "http://10.42.0.1:8787")
	spawner.compareAndClearEndpoint("s1", "http://10.42.0.1:8787")
	spawner.mu.RLock()
	_, stillThere := spawner.endpoints["s1"]
	spawner.mu.RUnlock()
	if stillThere {
		t.Fatal("stale endpoint not cleared")
	}

	// ...but a concurrent Spawn's fresh entry must survive an invalidation
	// triggered by an older Get that captured the previous value.
	spawner.setEndpoint("s2", "http://10.42.0.2:8787")
	spawner.compareAndClearEndpoint("s2", "http://10.42.0.1:8787")
	spawner.mu.RLock()
	fresh := spawner.endpoints["s2"]
	spawner.mu.RUnlock()
	if fresh != "http://10.42.0.2:8787" {
		t.Fatalf("fresh endpoint = %q, want http://10.42.0.2:8787 kept", fresh)
	}
}

func TestSpawnAdoptFailsWhenReplacementUnhealthy(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cfg := PodConfig{Namespace: "interactive-sessions"}.WithDefaults()
	tracker := cs.Tracker()
	// As with the adopt-success test, a new-generation pod takes the name
	// when the stale one is deleted — but its PodIP is unreachable, so the
	// adopt path's health check must fail the adopt instead of returning a
	// dead endpoint as success.
	cs.PrependReactor("delete", "pods", func(action fstesting.Action) (bool, runtime.Object, error) {
		del := action.(fstesting.DeleteAction)
		podsGVR := corev1.SchemeGroupVersion.WithResource("pods")
		_ = tracker.Delete(podsGVR, cfg.Namespace, del.GetName())
		concurrent := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: del.GetName(), Namespace: cfg.Namespace, UID: types.UID("uid-new-gen")},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				PodIP: "10.42.0.200",
				ContainerStatuses: []corev1.ContainerStatus{
					{Name: "agent", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
				},
			},
		}
		_ = tracker.Add(concurrent)
		return true, nil, nil
	})
	spawner := NewPodSpawner(cs, cfg, nil)
	spawner.deleteWait = 500 * time.Millisecond
	spawner.healthyWait = 500 * time.Millisecond
	// Health checks enabled (skipHealthCheck stays false).

	spec := hub.SpawnSpec{
		SessionID:   "sess-unhealthy-adopt",
		SessionKind: sessions.KindInteractive,
		Toolchain:   "golang",
		HubURL:      "http://session-hub.session-system.svc.cluster.local:8787",
		HubToken:    "tok-unhealthy-adopt",
	}
	stalePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: PodName(spec.SessionID), Namespace: cfg.Namespace, UID: types.UID("uid-old")},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.42.0.77",
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "agent", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}},
			},
		},
	}
	if _, err := cs.CoreV1().Pods(cfg.Namespace).Create(context.Background(), stalePod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create stale pod: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := spawner.Spawn(ctx, spec); err == nil {
		t.Fatal("Spawn adopted an unhealthy replacement pod, want a health check error")
	} else if !strings.Contains(err.Error(), "health check failed") {
		t.Fatalf("error = %q, want a replacement health check failure", err)
	}
	// The concurrent pod must not be deleted by the failed adopt (it is not
	// ours to Stop); the creator Spawn owns its lifecycle.
	got, err := cs.CoreV1().Pods(cfg.Namespace).Get(ctx, PodName(spec.SessionID), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get replacement pod: %v", err)
	}
	if got.UID != "uid-new-gen" {
		t.Fatalf("pod UID = %q, want uid-new-gen (failed adopt must not Stop the pod)", got.UID)
	}
}

func TestSpawnMutesWatcherForEntireCall(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cfg := PodConfig{Namespace: "interactive-sessions"}.WithDefaults()
	// Slow the token secret Create so Spawn stays in flight (past the
	// preflight Get and past any stale-pod cleanup) while the test
	// observes the replacement mute — the mute must cover the whole call,
	// not just the delete->create gap.
	cs.PrependReactor("create", "secrets", func(action fstesting.Action) (bool, runtime.Object, error) {
		time.Sleep(300 * time.Millisecond)
		return false, nil, nil
	})
	spawner := NewPodSpawner(cs, cfg, nil)
	spawner.skipHealthCheck = true

	spec := hub.SpawnSpec{
		SessionID:   "sess-mute-window",
		SessionKind: sessions.KindInteractive,
		Toolchain:   "golang",
		HubURL:      "http://session-hub.session-system.svc.cluster.local:8787",
		HubToken:    "tok-mute-window",
	}

	// Simulate kubelet assigning the fresh pod an IP.
	go func() {
		for i := 0; i < 100; i++ {
			time.Sleep(20 * time.Millisecond)
			pod, err := cs.CoreV1().Pods(cfg.Namespace).Get(context.Background(), PodName(spec.SessionID), metav1.GetOptions{})
			if err != nil || pod.Status.Phase != "" {
				continue
			}
			pod.Status.Phase = corev1.PodRunning
			pod.Status.PodIP = "10.42.0.60"
			_, _ = cs.CoreV1().Pods(cfg.Namespace).UpdateStatus(context.Background(), pod, metav1.UpdateOptions{})
			return
		}
	}()

	done := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		defer close(done)
		_, _ = spawner.Spawn(ctx, spec)
	}()

	// While Spawn is in flight (blocked in the slow secret Create), the
	// old generation's watcher must already be muted.
	time.Sleep(100 * time.Millisecond)
	if !spawner.isReplacing(spec.SessionID) {
		t.Fatal("replacement mute not held for the whole Spawn (not set during the in-flight call)")
	}
	<-done
	if spawner.isReplacing(spec.SessionID) {
		t.Fatal("replacement mark leaked after Spawn returned")
	}
}

func TestSpawnAdoptsConcurrentPodOnCreateAlreadyExists(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cfg := PodConfig{Namespace: "interactive-sessions"}.WithDefaults()
	stalePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: PodName("sess-create-aex"), Namespace: cfg.Namespace, UID: types.UID("uid-old")},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.42.0.77",
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "agent", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}},
			},
		},
	}
	if _, err := cs.CoreV1().Pods(cfg.Namespace).Create(context.Background(), stalePod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create stale pod: %v", err)
	}

	tracker := cs.Tracker()
	// The name is released when the stale pod is deleted, but by the time
	// this Spawn's own pod Create runs, a concurrent Spawn has created the
	// replacement: the Create returns AlreadyExists and a new-generation
	// pod now occupies the name.
	cs.PrependReactor("create", "pods", func(action fstesting.Action) (bool, runtime.Object, error) {
		create := action.(fstesting.CreateAction)
		obj := create.GetObject().(*corev1.Pod)
		concurrent := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: obj.Name, Namespace: cfg.Namespace, UID: types.UID("uid-new-gen")},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				PodIP: "10.42.0.200",
				ContainerStatuses: []corev1.ContainerStatus{
					{Name: "agent", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
				},
			},
		}
		_ = tracker.Add(concurrent)
		return true, nil, apierrors.NewAlreadyExists(corev1.Resource("pods"), obj.Name)
	})
	spawner := NewPodSpawner(cs, cfg, nil)
	spawner.skipHealthCheck = true

	spec := hub.SpawnSpec{
		SessionID:   "sess-create-aex",
		SessionKind: sessions.KindInteractive,
		Toolchain:   "golang",
		HubURL:      "http://session-hub.session-system.svc.cluster.local:8787",
		HubToken:    "tok-create-aex",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ep, err := spawner.Spawn(ctx, spec)
	if err != nil {
		t.Fatalf("Spawn with AlreadyExists pod Create failed: %v", err)
	}
	if ep != "http://10.42.0.200:8787" {
		t.Fatalf("endpoint = %q, want adopted http://10.42.0.200:8787", ep)
	}
	got, err := cs.CoreV1().Pods(cfg.Namespace).Get(ctx, PodName(spec.SessionID), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get replacement pod: %v", err)
	}
	if got.UID != "uid-new-gen" {
		t.Fatalf("pod UID = %q, want adopted uid-new-gen", got.UID)
	}
	// The token secret this Spawn (re)created must be bound to the adopted
	// pod for GC, not left orphaned.
	sec, err := cs.CoreV1().Secrets(cfg.Namespace).Get(ctx, TokenSecretName(spec.SessionID), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get token secret: %v", err)
	}
	if len(sec.OwnerReferences) != 1 || sec.OwnerReferences[0].UID != "uid-new-gen" || sec.OwnerReferences[0].Kind != "Pod" {
		t.Fatalf("secret owner refs = %+v, want exactly one Pod ref with UID uid-new-gen", sec.OwnerReferences)
	}
}

func TestSpawnAEXAdoptFailureKeepsSharedSecret(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cfg := PodConfig{Namespace: "interactive-sessions"}.WithDefaults()
	if _, err := cs.CoreV1().Pods(cfg.Namespace).Create(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: PodName("sess-aex-fail"), Namespace: cfg.Namespace, UID: types.UID("uid-old")},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.42.0.77",
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "agent", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}},
			},
		},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create stale pod: %v", err)
	}
	tracker := cs.Tracker()
	cs.PrependReactor("create", "pods", func(action fstesting.Action) (bool, runtime.Object, error) {
		create := action.(fstesting.CreateAction)
		obj := create.GetObject().(*corev1.Pod)
		// The concurrent replacement has an IP but is unreachable, so the
		// adopt's health check fails.
		concurrent := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: obj.Name, Namespace: cfg.Namespace, UID: types.UID("uid-new-gen")},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				PodIP: "10.42.0.200",
				ContainerStatuses: []corev1.ContainerStatus{
					{Name: "agent", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
				},
			},
		}
		_ = tracker.Add(concurrent)
		return true, nil, apierrors.NewAlreadyExists(corev1.Resource("pods"), obj.Name)
	})
	spawner := NewPodSpawner(cs, cfg, nil)
	spawner.healthyWait = 500 * time.Millisecond
	// Health checks enabled (skipHealthCheck stays false).

	spec := hub.SpawnSpec{
		SessionID:   "sess-aex-fail",
		SessionKind: sessions.KindInteractive,
		Toolchain:   "golang",
		HubURL:      "http://session-hub.session-system.svc.cluster.local:8787",
		HubToken:    "tok-aex-fail",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := spawner.Spawn(ctx, spec); err == nil {
		t.Fatal("Spawn succeeded, want the failed adopt to surface an error")
	} else if !strings.Contains(err.Error(), "health check failed") {
		t.Fatalf("error = %q, want a replacement health check failure", err)
	}
	// The shared token secret must survive a failed adopt: the adopted pod
	// is still alive (its creator owns its lifecycle) and authenticates
	// against it, so deleting it would brick the replacement session. It
	// must also stay bound to the pod for GC.
	sec, err := cs.CoreV1().Secrets(cfg.Namespace).Get(ctx, TokenSecretName(spec.SessionID), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("token secret after failed adopt: %v, want kept (pod still alive)", err)
	}
	if len(sec.OwnerReferences) != 1 || sec.OwnerReferences[0].UID != "uid-new-gen" || sec.OwnerReferences[0].Kind != "Pod" {
		t.Fatalf("secret owner refs = %+v, want exactly one Pod ref with UID uid-new-gen", sec.OwnerReferences)
	}
}

func TestSpawnReusesConcurrentSecretOnCreateAlreadyExists(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cfg := PodConfig{Namespace: "interactive-sessions"}.WithDefaults()
	tracker := cs.Tracker()
	// A concurrent Spawn creates the token secret between our Delete and
	// Create; the Create returns AlreadyExists.
	cs.PrependReactor("create", "secrets", func(action fstesting.Action) (bool, runtime.Object, error) {
		concurrent := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: TokenSecretName("sess-secret-aex"), Namespace: cfg.Namespace},
			Type:       corev1.SecretTypeOpaque,
			Data:       map[string][]byte{"token": []byte("tok-other-gen")},
		}
		_ = tracker.Add(concurrent)
		return true, nil, apierrors.NewAlreadyExists(corev1.Resource("secrets"), concurrent.Name)
	})
	spawner := NewPodSpawner(cs, cfg, nil)
	spawner.skipHealthCheck = true

	spec := hub.SpawnSpec{
		SessionID:   "sess-secret-aex",
		SessionKind: sessions.KindInteractive,
		Toolchain:   "golang",
		HubURL:      "http://session-hub.session-system.svc.cluster.local:8787",
		HubToken:    "tok-this-spawn",
	}

	// Simulate kubelet assigning the pod an IP.
	go func() {
		for i := 0; i < 100; i++ {
			time.Sleep(20 * time.Millisecond)
			pod, err := cs.CoreV1().Pods(cfg.Namespace).Get(context.Background(), PodName(spec.SessionID), metav1.GetOptions{})
			if err != nil || pod.Status.Phase != "" {
				continue
			}
			pod.Status.Phase = corev1.PodRunning
			pod.Status.PodIP = "10.42.0.61"
			_, _ = cs.CoreV1().Pods(cfg.Namespace).UpdateStatus(context.Background(), pod, metav1.UpdateOptions{})
			return
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := spawner.Spawn(ctx, spec); err != nil {
		t.Fatalf("Spawn with AlreadyExists secret Create failed: %v", err)
	}
	// The concurrent secret's content must be preserved (not clobbered by
	// this Spawn's token): the pod reads it at container start.
	sec, err := cs.CoreV1().Secrets(cfg.Namespace).Get(ctx, TokenSecretName(spec.SessionID), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get token secret: %v", err)
	}
	if string(sec.Data["token"]) != "tok-other-gen" {
		t.Fatalf("secret token = %q, want the concurrent tok-other-gen preserved", string(sec.Data["token"]))
	}
	if len(sec.OwnerReferences) != 1 || sec.OwnerReferences[0].Kind != "Pod" || sec.OwnerReferences[0].Name != PodName(spec.SessionID) {
		t.Fatalf("secret owner refs = %+v, want exactly one Pod ref for GC", sec.OwnerReferences)
	}
}

func TestSpawnPodCreateFailureKeepsReusedSecret(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cfg := PodConfig{Namespace: "interactive-sessions"}.WithDefaults()
	tracker := cs.Tracker()
	cs.PrependReactor("create", "secrets", func(action fstesting.Action) (bool, runtime.Object, error) {
		concurrent := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: TokenSecretName("sess-pod-fail"), Namespace: cfg.Namespace},
			Type:       corev1.SecretTypeOpaque,
			Data:       map[string][]byte{"token": []byte("tok-other-gen")},
		}
		_ = tracker.Add(concurrent)
		return true, nil, apierrors.NewAlreadyExists(corev1.Resource("secrets"), concurrent.Name)
	})
	cs.PrependReactor("create", "pods", func(action fstesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewInternalError(fmt.Errorf("api server unavailable"))
	})
	spawner := NewPodSpawner(cs, cfg, nil)

	spec := hub.SpawnSpec{
		SessionID:   "sess-pod-fail",
		SessionKind: sessions.KindInteractive,
		Toolchain:   "golang",
		HubURL:      "http://session-hub.session-system.svc.cluster.local:8787",
		HubToken:    "tok-this-spawn",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := spawner.Spawn(ctx, spec); err == nil {
		t.Fatal("Spawn succeeded, want the pod Create failure to surface")
	} else if !strings.Contains(err.Error(), "api server unavailable") {
		t.Fatalf("error = %q, want the pod Create failure", err)
	}
	// The secret was reused from the concurrent Spawn, so the failed pod
	// Create must not delete it (it may be bound to a healthy concurrent
	// pod that authenticates against it).
	sec, err := cs.CoreV1().Secrets(cfg.Namespace).Get(ctx, TokenSecretName(spec.SessionID), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("token secret after failed pod Create: %v, want kept (reused, not ours)", err)
	}
	if string(sec.Data["token"]) != "tok-other-gen" {
		t.Fatalf("secret token = %q, want tok-other-gen intact", string(sec.Data["token"]))
	}
}

func TestWaitDeletedReturnsEarlyOnForeignUID(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cfg := PodConfig{Namespace: "interactive-sessions"}.WithDefaults()
	tracker := cs.Tracker()
	cs.PrependReactor("delete", "pods", func(action fstesting.Action) (bool, runtime.Object, error) {
		del := action.(fstesting.DeleteAction)
		podsGVR := corev1.SchemeGroupVersion.WithResource("pods")
		_ = tracker.Delete(podsGVR, cfg.Namespace, del.GetName())
		concurrent := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: del.GetName(), Namespace: cfg.Namespace, UID: types.UID("uid-new-gen")},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				PodIP: "10.42.0.200",
				ContainerStatuses: []corev1.ContainerStatus{
					{Name: "agent", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
				},
			},
		}
		_ = tracker.Add(concurrent)
		return true, nil, nil
	})
	spawner := NewPodSpawner(cs, cfg, nil)
	spawner.skipHealthCheck = true
	// A long delete deadline: if waitDeleted does not return early on the
	// foreign-UID pod, the short ctx below expires first and Spawn fails.
	spawner.deleteWait = 30 * time.Second

	spec := hub.SpawnSpec{
		SessionID:   "sess-early-wait",
		SessionKind: sessions.KindInteractive,
		Toolchain:   "golang",
		HubURL:      "http://session-hub.session-system.svc.cluster.local:8787",
		HubToken:    "tok-early-wait",
	}
	stalePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: PodName(spec.SessionID), Namespace: cfg.Namespace, UID: types.UID("uid-old")},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.42.0.77",
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "agent", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}},
			},
		},
	}
	if _, err := cs.CoreV1().Pods(cfg.Namespace).Create(context.Background(), stalePod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create stale pod: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	ep, err := spawner.Spawn(ctx, spec)
	if err != nil {
		t.Fatalf("Spawn failed: %v (waitDeleted did not return early on the foreign-UID pod?)", err)
	}
	if ep != "http://10.42.0.200:8787" {
		t.Fatalf("endpoint = %q, want adopted http://10.42.0.200:8787", ep)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("Spawn took %v, want well under the 30s deleteWait deadline", d)
	}
}

func TestReplacingMarkRefcountsOverlappingSpawns(t *testing.T) {
	spawner := NewPodSpawner(fake.NewSimpleClientset(), PodConfig{Namespace: "interactive-sessions"}.WithDefaults(), nil)
	sessionID := "sess-refcount"

	if spawner.isReplacing(sessionID) {
		t.Fatal("isReplacing = true before any mark")
	}
	// Two overlapping Spawns for the same session: the first one's deferred
	// unmark must not lift the mute while the second is still in its gap.
	spawner.markReplacing(sessionID)
	spawner.markReplacing(sessionID)
	spawner.unmarkReplacing(sessionID)
	if !spawner.isReplacing(sessionID) {
		t.Fatal("isReplacing = false after one of two overlapping marks cleared")
	}
	spawner.unmarkReplacing(sessionID)
	if spawner.isReplacing(sessionID) {
		t.Fatal("isReplacing = true after all marks cleared")
	}
	// Unmark with no outstanding mark is a no-op (never drives count negative).
	spawner.unmarkReplacing(sessionID)
	spawner.markReplacing(sessionID)
	if !spawner.isReplacing(sessionID) {
		t.Fatal("isReplacing = false after mark")
	}
	spawner.unmarkReplacing(sessionID)
	if spawner.isReplacing(sessionID) {
		t.Fatal("isReplacing = true after mark cleared")
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
