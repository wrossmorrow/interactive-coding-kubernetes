package k8s

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"github.com/wrossmorrow/k8s-agents/pkg/hub"
)

// PodSpawner implements hub.Spawner by managing per-session sandbox Pods in
// Kubernetes (GKE Autopilot).
type PodSpawner struct {
	client          kubernetes.Interface
	cfg             PodConfig
	onExit          func(sessionID string)
	skipHealthCheck bool // enabled in fake-clientset unit tests

	mu        sync.RWMutex
	endpoints map[string]string // sessionID -> http://<podIP>:<port>
	replacing map[string]bool   // sessionID -> Spawn is mid delete->create replacement

	// Polling tunables, overridable in unit tests.
	exitPoll   time.Duration // watchExit poll interval
	deleteWait time.Duration // waitDeleted deadline
}

// NewPodSpawner constructs a Kubernetes PodSpawner.
func NewPodSpawner(client kubernetes.Interface, cfg PodConfig, onExit func(sessionID string)) *PodSpawner {
	return &PodSpawner{
		client:     client,
		cfg:        cfg.WithDefaults(),
		onExit:     onExit,
		endpoints:  map[string]string{},
		replacing:  map[string]bool{},
		exitPoll:   3 * time.Second,
		deleteWait: 45 * time.Second,
	}
}

func (p *PodSpawner) ensureNamespacePolicies(ctx context.Context, hubURL string) error {
	cm := BuildEgressConfigMap(p.cfg, hubURL)
	if existing, err := p.client.CoreV1().ConfigMaps(p.cfg.Namespace).Get(ctx, cm.Name, metav1.GetOptions{}); apierrors.IsNotFound(err) {
		if _, cerr := p.client.CoreV1().ConfigMaps(p.cfg.Namespace).Create(ctx, cm, metav1.CreateOptions{}); cerr != nil && !apierrors.IsAlreadyExists(cerr) {
			return fmt.Errorf("create egress configmap: %w", cerr)
		}
	} else if err == nil && existing.Data["squid.conf"] != cm.Data["squid.conf"] {
		existing.Data = cm.Data
		if _, uerr := p.client.CoreV1().ConfigMaps(p.cfg.Namespace).Update(ctx, existing, metav1.UpdateOptions{}); uerr != nil {
			return fmt.Errorf("update egress configmap: %w", uerr)
		}
	}

	np := BuildSandboxNetworkPolicy(p.cfg)
	if _, err := p.client.NetworkingV1().NetworkPolicies(p.cfg.Namespace).Get(ctx, np.Name, metav1.GetOptions{}); apierrors.IsNotFound(err) {
		if _, cerr := p.client.NetworkingV1().NetworkPolicies(p.cfg.Namespace).Create(ctx, np, metav1.CreateOptions{}); cerr != nil && !apierrors.IsAlreadyExists(cerr) {
			return fmt.Errorf("create sandbox networkpolicy: %w", cerr)
		}
	}
	return nil
}

func (p *PodSpawner) Spawn(ctx context.Context, spec hub.SpawnSpec) (string, error) {
	if ep, ok := p.Endpoint(spec.SessionID); ok {
		return ep, nil
	}
	if err := p.ensureNamespacePolicies(ctx, spec.HubURL); err != nil {
		return "", err
	}

	ns := p.cfg.Namespace
	podName := PodName(spec.SessionID)

	// Clean up any prior Pod that is not actively running (completed/failed,
	// or a Running pod whose agent container terminated while a native
	// sidecar is still shutting down). The agent is already dead in both
	// cases, so force-delete to release the pod name quickly; waitDeleted is
	// a best-effort bound below.
	if existing, err := p.client.CoreV1().Pods(ns).Get(ctx, podName, metav1.GetOptions{}); err == nil {
		if podAgentRunning(existing) {
			ep := fmt.Sprintf("http://%s:%d", existing.Status.PodIP, p.cfg.SessiondPort)
			p.setEndpoint(spec.SessionID, ep)
			return ep, nil
		}
		// Mark the replacement in flight so the old generation's watchExit
		// does not report exit during the delete->create name gap (which
		// can outlast its NotFound debounce under API latency); cleared
		// whenever this Spawn returns.
		p.markReplacing(spec.SessionID)
		defer p.unmarkReplacing(spec.SessionID)

		graceZero := int64(0)
		if derr := p.client.CoreV1().Pods(ns).Delete(ctx, podName, metav1.DeleteOptions{GracePeriodSeconds: &graceZero}); derr != nil && !apierrors.IsNotFound(derr) {
			log.Printf("force-delete stale sandbox pod %s: %v", podName, derr)
		}
		p.waitDeleted(ctx, podName)
		// waitDeleted is a bounded best-effort wait; if the old pod still
		// holds the name, fail with a clear cause instead of a confusing
		// AlreadyExists from the Create below.
		if remaining, gerr := p.client.CoreV1().Pods(ns).Get(ctx, podName, metav1.GetOptions{}); gerr == nil {
			return "", fmt.Errorf("stale sandbox pod %s (phase %s) still exists after force-delete; retry once the deletion completes", podName, remaining.Status.Phase)
		}
	}

	// Create or replace the per-session scoped bearer token Secret.
	sec := BuildTokenSecret(ns, spec.SessionID, spec.SessionKind, spec.HubToken, spec.AnalystToken)
	_ = p.client.CoreV1().Secrets(ns).Delete(ctx, sec.Name, metav1.DeleteOptions{})
	createdSec, err := p.client.CoreV1().Secrets(ns).Create(ctx, sec, metav1.CreateOptions{})
	if err != nil {
		return "", fmt.Errorf("create session token secret: %w", err)
	}

	pod := BuildSandboxPod(p.cfg, spec)
	createdPod, err := p.client.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		_ = p.client.CoreV1().Secrets(ns).Delete(context.Background(), sec.Name, metav1.DeleteOptions{})
		return "", fmt.Errorf("create sandbox pod: %w", err)
	}

	// Bind Secret lifetime to the Pod via OwnerReferences for automatic GC.
	createdSec.OwnerReferences = []metav1.OwnerReference{
		{
			APIVersion: "v1",
			Kind:       "Pod",
			Name:       createdPod.Name,
			UID:        createdPod.UID,
		},
	}
	_, _ = p.client.CoreV1().Secrets(ns).Update(ctx, createdSec, metav1.UpdateOptions{})

	podIP, err := p.waitPodIP(ctx, podName)
	if err != nil {
		_ = p.Stop(context.Background(), spec.SessionID)
		return "", err
	}
	endpoint := fmt.Sprintf("http://%s:%d", podIP, p.cfg.SessiondPort)
	if !p.skipHealthCheck {
		if err := waitPodHealthy(ctx, endpoint+"/api/health"); err != nil {
			_ = p.Stop(context.Background(), spec.SessionID)
			return "", fmt.Errorf("sandbox pod health check failed: %w", err)
		}
	}

	p.setEndpoint(spec.SessionID, endpoint)
	go p.watchExit(spec.SessionID, podName, createdPod.UID)
	return endpoint, nil
}

func (p *PodSpawner) Endpoint(sessionID string) (string, bool) {
	var cached string
	p.mu.RLock()
	if ep, ok := p.endpoints[sessionID]; ok {
		cached = ep
	}
	p.mu.RUnlock()

	// Verify against Kubernetes (also the fallback lookup path when no
	// cached endpoint exists, so session-hub restarts seamlessly rediscover
	// already-running sandbox Pods).
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	pod, err := p.client.CoreV1().Pods(p.cfg.Namespace).Get(ctx, PodName(sessionID), metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		// Pod is gone; drop any stale cached endpoint.
		if cached != "" {
			p.clearEndpoint(sessionID)
		}
		return "", false
	case err == nil && !podAgentRunning(pod):
		// Agent terminated or pod leaving: drop the stale cached entry
		// before watchExit's next tick would clear it.
		if cached != "" {
			p.clearEndpoint(sessionID)
		}
		return "", false
	case err == nil:
		ep := fmt.Sprintf("http://%s:%d", pod.Status.PodIP, p.cfg.SessiondPort)
		p.setEndpoint(sessionID, ep)
		return ep, true
	}
	// Transient API error: keep returning the recently-validated cached
	// endpoint if we have one.
	if cached != "" {
		return cached, true
	}
	return "", false
}

func (p *PodSpawner) Running(sessionID string) bool {
	_, ok := p.Endpoint(sessionID)
	return ok
}

func (p *PodSpawner) WaitExit(ctx context.Context, sessionID string) {
	podName := PodName(sessionID)
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		pod, err := p.client.CoreV1().Pods(p.cfg.Namespace).Get(ctx, podName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			p.clearEndpoint(sessionID)
			return
		}
		if err == nil && podAgentTerminated(pod) {
			p.clearEndpoint(sessionID)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func (p *PodSpawner) Stop(ctx context.Context, sessionID string) error {
	p.clearEndpoint(sessionID)
	grace := int64(30)
	err := p.client.CoreV1().Pods(p.cfg.Namespace).Delete(ctx, PodName(sessionID), metav1.DeleteOptions{
		GracePeriodSeconds: &grace,
	})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// CloseAll is intentionally a no-op for K8s: restarting session-hub must not
// terminate developers' active interactive sandbox Pods.
func (p *PodSpawner) CloseAll() error { return nil }

// LookupToken recovers the per-session bearer token from its Kubernetes Secret
// so session-hub restarts seamlessly re-authenticate to already-running Pods.
func (p *PodSpawner) LookupToken(sessionID string) (string, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	sec, err := p.client.CoreV1().Secrets(p.cfg.Namespace).Get(ctx, TokenSecretName(sessionID), metav1.GetOptions{})
	if err != nil {
		return "", false
	}
	if tok := string(sec.Data["token"]); tok != "" {
		return tok, true
	}
	return "", false
}

// LookupAnalystToken retrieves the read-only analyst token for sessionID directly
// from its named Kubernetes Secret (requiring only Secret get, not list) so
// session-hub restarts seamlessly keep analyst pods authorized.
func (p *PodSpawner) LookupAnalystToken(sessionID string) (string, bool) {
	if sessionID == "" {
		return "", false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	sec, err := p.client.CoreV1().Secrets(p.cfg.Namespace).Get(ctx, TokenSecretName(sessionID), metav1.GetOptions{})
	if err != nil {
		return "", false
	}
	if tok := string(sec.Data["analyst-token"]); tok != "" {
		return tok, true
	}
	return "", false
}

func (p *PodSpawner) setEndpoint(sessionID, ep string) {
	p.mu.Lock()
	p.endpoints[sessionID] = ep
	p.mu.Unlock()
}

func (p *PodSpawner) clearEndpoint(sessionID string) {
	p.mu.Lock()
	delete(p.endpoints, sessionID)
	p.mu.Unlock()
}

// markReplacing, unmarkReplacing, and isReplacing track whether Spawn is mid
// delete->create replacement for a session, so the old pod generation's
// watchExit can distinguish an intentional replacement gap from a real exit.
func (p *PodSpawner) markReplacing(sessionID string) {
	p.mu.Lock()
	p.replacing[sessionID] = true
	p.mu.Unlock()
}

func (p *PodSpawner) unmarkReplacing(sessionID string) {
	p.mu.Lock()
	delete(p.replacing, sessionID)
	p.mu.Unlock()
}

func (p *PodSpawner) isReplacing(sessionID string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.replacing[sessionID]
}

func (p *PodSpawner) waitPodIP(ctx context.Context, podName string) (string, error) {
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		pod, err := p.client.CoreV1().Pods(p.cfg.Namespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			return "", fmt.Errorf("get sandbox pod %s: %w", podName, err)
		}
		switch pod.Status.Phase {
		case corev1.PodRunning:
			if pod.Status.PodIP != "" {
				return pod.Status.PodIP, nil
			}
		case corev1.PodFailed, corev1.PodSucceeded:
			return "", fmt.Errorf("sandbox pod %s exited with phase %s", podName, pod.Status.Phase)
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-tick.C:
		}
	}
}

func (p *PodSpawner) waitDeleted(ctx context.Context, podName string) {
	// Deadline runs past the default 30s pod termination grace period so a
	// slow sidecar exit does not leave the pod name held when Spawn creates.
	deadline := time.NewTimer(p.deleteWait)
	defer deadline.Stop()
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		if _, err := p.client.CoreV1().Pods(p.cfg.Namespace).Get(ctx, podName, metav1.GetOptions{}); apierrors.IsNotFound(err) {
			return
		}
		select {
		case <-deadline.C:
			return
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// watchExit polls the sandbox pod until the agent container exits or the pod
// is deleted, then clears the endpoint and notifies onExit. It only reacts
// to the pod generation it was started for (podUID): if Spawn has replaced
// the pod, the name belongs to a new generation with its own watcher, and
// this stale watcher stops silently.
func (p *PodSpawner) watchExit(sessionID, podName string, podUID types.UID) {
	missing := 0
	tick := time.NewTicker(p.exitPoll)
	defer tick.Stop()
	for range tick.C {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		pod, err := p.client.CoreV1().Pods(p.cfg.Namespace).Get(ctx, podName, metav1.GetOptions{})
		cancel()
		if err == nil {
			if pod.UID != "" && podUID != "" && pod.UID != podUID {
				// Pod name is now owned by a new generation; the new watcher
				// owns exit handling for this session.
				return
			}
			missing = 0
			if p.isReplacing(sessionID) {
				// A replacement is in flight for this session; the new
				// generation gets its own watcher on success, so stay silent
				// instead of reporting the old generation's exit.
				continue
			}
			if podAgentTerminated(pod) {
				p.clearEndpoint(sessionID)
				if p.onExit != nil {
					p.onExit(sessionID)
				}
				return
			}
			continue
		}
		if apierrors.IsNotFound(err) {
			// The pod may be in the delete->create gap of a replacement by
			// Spawn, which can outlast the debounce below under API latency.
			if p.isReplacing(sessionID) {
				missing = 0
				continue
			}
			// Confirm on the next tick.
			missing++
			if missing < 2 {
				continue
			}
			p.clearEndpoint(sessionID)
			if p.onExit != nil {
				p.onExit(sessionID)
			}
			return
		}
		log.Printf("watch sandbox pod %s: %v", podName, err)
	}
}

// podAgentTerminated reports whether the pod has finished or its primary "agent"
// container has terminated (even if native sidecars are still shutting down or
// in CrashLoopBackOff, which keeps pod.Status.Phase == PodRunning).
func podAgentTerminated(pod *corev1.Pod) bool {
	if pod == nil {
		return true
	}
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return true
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name == "agent" && cs.State.Terminated != nil {
			return true
		}
	}
	return false
}

// podAgentRunning reports whether the pod is live and its "agent" container has
// not terminated.
func podAgentRunning(pod *corev1.Pod) bool {
	if pod == nil || pod.DeletionTimestamp != nil {
		return false
	}
	return pod.Status.Phase == corev1.PodRunning && pod.Status.PodIP != "" && !podAgentTerminated(pod)
}

func waitPodHealthy(ctx context.Context, url string) error {
	client := &http.Client{Timeout: 1 * time.Second}
	deadline := time.NewTimer(60 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if resp, err := client.Do(req); err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-deadline.C:
			return fmt.Errorf("timed out waiting for %s", url)
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}
