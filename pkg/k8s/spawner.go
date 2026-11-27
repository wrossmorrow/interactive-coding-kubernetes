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
}

// NewPodSpawner constructs a Kubernetes PodSpawner.
func NewPodSpawner(client kubernetes.Interface, cfg PodConfig, onExit func(sessionID string)) *PodSpawner {
	return &PodSpawner{
		client:    client,
		cfg:       cfg.WithDefaults(),
		onExit:    onExit,
		endpoints: map[string]string{},
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

	// Clean up any prior completed/failed Pod from an earlier pause cycle.
	if existing, err := p.client.CoreV1().Pods(ns).Get(ctx, podName, metav1.GetOptions{}); err == nil {
		if existing.Status.Phase == corev1.PodRunning && existing.Status.PodIP != "" {
			ep := fmt.Sprintf("http://%s:%d", existing.Status.PodIP, p.cfg.SessiondPort)
			p.setEndpoint(spec.SessionID, ep)
			return ep, nil
		}
		_ = p.client.CoreV1().Pods(ns).Delete(ctx, podName, metav1.DeleteOptions{})
		p.waitDeleted(ctx, podName)
	}

	// Create or replace the per-session scoped bearer token Secret.
	sec := BuildTokenSecret(ns, spec.SessionID, spec.HubToken)
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
	go p.watchExit(spec.SessionID, podName)
	return endpoint, nil
}

func (p *PodSpawner) Endpoint(sessionID string) (string, bool) {
	p.mu.RLock()
	ep, ok := p.endpoints[sessionID]
	p.mu.RUnlock()
	if ok {
		return ep, true
	}
	// Fallback lookup in Kubernetes so session-hub restarts seamlessly
	// rediscover already-running sandbox Pods.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	pod, err := p.client.CoreV1().Pods(p.cfg.Namespace).Get(ctx, PodName(sessionID), metav1.GetOptions{})
	if err != nil {
		return "", false
	}
	if pod.Status.Phase == corev1.PodRunning && pod.Status.PodIP != "" && pod.DeletionTimestamp == nil {
		ep = fmt.Sprintf("http://%s:%d", pod.Status.PodIP, p.cfg.SessiondPort)
		p.setEndpoint(sessionID, ep)
		return ep, true
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
		if err == nil && (pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed) {
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
	deadline := time.NewTimer(15 * time.Second)
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

func (p *PodSpawner) watchExit(sessionID, podName string) {
	tick := time.NewTicker(3 * time.Second)
	defer tick.Stop()
	for range tick.C {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		pod, err := p.client.CoreV1().Pods(p.cfg.Namespace).Get(ctx, podName, metav1.GetOptions{})
		cancel()
		if apierrors.IsNotFound(err) || (err == nil && (pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed)) {
			p.clearEndpoint(sessionID)
			if p.onExit != nil {
				p.onExit(sessionID)
			}
			return
		}
		if err != nil {
			log.Printf("watch sandbox pod %s: %v", podName, err)
		}
	}
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
