// Package k8s implements the Kubernetes PodSpawner and manifest renderers for
// interactive pi sandboxes on GKE Autopilot.
package k8s

import (
	"fmt"
	"net/url"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/wrossmorrow/k8s-agents/pkg/hub"
	"github.com/wrossmorrow/k8s-agents/pkg/sessions"
)

const (
	LabelApp       = "app.kubernetes.io/name"
	LabelComponent = "app.kubernetes.io/component"
	LabelSessionID = "k8s-agents.io/session-id"
	LabelKind      = "k8s-agents.io/session-kind"
	LabelToolchain = "k8s-agents.io/toolchain"

	DefaultNamespace           = "interactive-sessions"
	DefaultSessionHubNamespace = "session-system"
	DefaultAIGatewayNamespace  = "ai-gateway"
	DefaultSessiondPort        = 8787
	DefaultProxyPort           = 3128
	DefaultIdlePauseSeconds    = 1200 // 20 minutes
	EgressConfigMapName        = "sandbox-egress-config"
	NetworkPolicyName          = "sandbox-isolation"
)

// DefaultEgressAllowlist covers GitHub and standard language package registries.
// Note: Squid 5.x rejects overlapping dstdomain entries (e.g. both "github.com"
// and ".github.com"), so wildcard entries like ".github.com" must not be paired
// with their apex or subdomains.
var DefaultEgressAllowlist = []string{
	".github.com",
	".githubusercontent.com",
	"proxy.golang.org",
	"sum.golang.org",
	// proxy.golang.org redirects module zip downloads to storage.googleapis.com.
	// Accepted tradeoff: Squid dstdomain is host-only (no path ACL over HTTPS
	// CONNECT), so sandboxes can read public GCS objects; private buckets remain
	// inaccessible because sandbox pods hold zero GCP credentials / WI.
	"storage.googleapis.com",
	"pypi.org",
	"files.pythonhosted.org",
	"crates.io",
	"static.crates.io",
	"registry.npmjs.org",
}

// PodConfig holds cluster-wide settings for rendered interactive sandbox Pods.
type PodConfig struct {
	Namespace              string
	SessionHubNamespace    string
	AIGatewayNamespace     string
	ImageRepository        string
	ImagePrefix            string
	ImageTag               string
	ImagePullPolicy        corev1.PullPolicy
	ProxyImage             string
	GitHubAuthImage        string
	GatewayEndpoint        string
	GatewaySecretName      string
	GatewaySecretKey       string
	GitHubAppSecretName    string
	GitHubAppConfigMapName string
	ModelCatalogConfigMap  string
	EgressAllowlist        []string
	SessiondPort           int32
	ProxyPort              int32
	IdlePauseSeconds       int
	FlushIntervalSeconds   int
	ServiceAccountName     string
}

// WithDefaults fills unset fields with hardened GKE Autopilot defaults.
func (c PodConfig) WithDefaults() PodConfig {
	if c.Namespace == "" {
		c.Namespace = DefaultNamespace
	}
	if c.SessionHubNamespace == "" {
		c.SessionHubNamespace = DefaultSessionHubNamespace
	}
	if c.AIGatewayNamespace == "" {
		c.AIGatewayNamespace = DefaultAIGatewayNamespace
	}
	if c.ImageRepository == "" {
		c.ImageRepository = "ghcr.io/wrossmorrow"
	}
	if c.ImagePrefix == "" {
		c.ImagePrefix = "k8s-agents-sandbox"
	}
	if c.ImageTag == "" {
		c.ImageTag = "latest"
	}
	if c.ImagePullPolicy == "" {
		c.ImagePullPolicy = corev1.PullIfNotPresent
	}
	if c.ProxyImage == "" {
		c.ProxyImage = "ubuntu/squid:5.2-22.04_beta"
	}
	if c.GitHubAuthImage == "" {
		c.GitHubAuthImage = "ghcr.io/wrossmorrow/agentrunner-github-auth:latest"
	}
	if c.GatewayEndpoint == "" {
		c.GatewayEndpoint = "http://agent-router.ai-gateway.svc.cluster.local:8080/v1"
	}
	if c.GatewaySecretKey == "" {
		c.GatewaySecretKey = "token"
	}
	if c.SessiondPort == 0 {
		c.SessiondPort = DefaultSessiondPort
	}
	if c.ProxyPort == 0 {
		c.ProxyPort = DefaultProxyPort
	}
	if c.IdlePauseSeconds == 0 {
		c.IdlePauseSeconds = DefaultIdlePauseSeconds
	}
	if c.FlushIntervalSeconds == 0 {
		c.FlushIntervalSeconds = 30
	}
	if len(c.EgressAllowlist) == 0 {
		c.EgressAllowlist = append([]string(nil), DefaultEgressAllowlist...)
	}
	return c
}

// PodName returns a deterministic DNS-1123 compliant Pod name for sessionID.
func PodName(sessionID string) string {
	clean := strings.ToLower(sessionID)
	var b strings.Builder
	for _, r := range clean {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			b.WriteRune(r)
		}
	}
	s := strings.Trim(b.String(), "-")
	if len(s) > 48 {
		s = s[:48]
	}
	if s == "" {
		s = "session"
	}
	return "pi-sess-" + s
}

// TokenSecretName returns the per-session Secret name storing SESSION_HUB_TOKEN.
func TokenSecretName(sessionID string) string {
	return PodName(sessionID) + "-token"
}

// SandboxImage returns the thin sandbox image ref for the requested toolchain.
func (c PodConfig) SandboxImage(toolchain string) string {
	tc := strings.ToLower(strings.TrimSpace(toolchain))
	switch tc {
	case "python", "golang", "rust", "java17", "java21", "agent":
	default:
		tc = "agent"
	}
	return fmt.Sprintf("%s/%s-%s:%s", strings.TrimRight(c.ImageRepository, "/"), c.ImagePrefix, tc, c.ImageTag)
}

// BuildTokenSecret constructs the per-session scoped bearer token Secret.
// For analyst sessions it also stores the separate read-only analyst-token key.
func BuildTokenSecret(namespace, sessionID string, kind sessions.Kind, token, analystToken string) *corev1.Secret {
	labels := map[string]string{
		LabelApp:       "pi-sessiond",
		LabelComponent: "sandbox",
		LabelSessionID: sessionID,
	}
	data := map[string][]byte{
		"token": []byte(token),
	}
	if kind == sessions.KindAnalyst && analystToken != "" {
		labels[LabelKind] = string(sessions.KindAnalyst)
		data["analyst-token"] = []byte(analystToken)
	}
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      TokenSecretName(sessionID),
			Namespace: namespace,
			Labels:    labels,
		},
		Type: corev1.SecretTypeOpaque,
		Data: data,
	}
}

// BuildEgressConfigMap renders squid.conf for the per-pod egress-proxy sidecar.
func BuildEgressConfigMap(cfg PodConfig, hubURL string) *corev1.ConfigMap {
	cfg = cfg.WithDefaults()
	seen := map[string]struct{}{}
	var domains []string
	addHost := func(h string) {
		h = strings.ToLower(strings.TrimSpace(h))
		if h == "" {
			return
		}
		if _, ok := seen[h]; ok {
			return
		}
		seen[h] = struct{}{}
		domains = append(domains, h)
	}
	for _, d := range cfg.EgressAllowlist {
		addHost(d)
	}
	addHost(urlHost(cfg.GatewayEndpoint))
	addHost(urlHost(hubURL))
	sort.Strings(domains)

	var b strings.Builder
	fmt.Fprintf(&b, "http_port 127.0.0.1:%d\n", cfg.ProxyPort)
	b.WriteString("max_filedescriptors 1024\n")
	b.WriteString("pid_filename /tmp/squid.pid\n")
	b.WriteString("access_log stdio:/var/log/squid/access.log\n")
	b.WriteString("cache_log /var/log/squid/cache.log\n")
	b.WriteString("cache deny all\n")
	for _, d := range domains {
		fmt.Fprintf(&b, "acl allowed_domains dstdomain %s\n", d)
	}
	b.WriteString("http_access allow allowed_domains\n")
	b.WriteString("http_access deny all\n")

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      EgressConfigMapName,
			Namespace: cfg.Namespace,
			Labels: map[string]string{
				LabelApp:       "pi-sessiond",
				LabelComponent: "sandbox-egress",
			},
		},
		Data: map[string]string{
			"squid.conf": b.String(),
		},
	}
}

// BuildSandboxNetworkPolicy creates the GKE Dataplane V2 NetworkPolicy for
// interactive sandbox pods:
//   - Ingress ONLY from session-hub in session-system on SessiondPort (no lateral traffic).
//   - Egress ONLY to DNS (53), ai-gateway (8080), session-system (8787), and
//     external HTTPS (443) explicitly excluding 169.254.169.254/32 and RFC1918 CIDRs.
func BuildSandboxNetworkPolicy(cfg PodConfig) *networkingv1.NetworkPolicy {
	cfg = cfg.WithDefaults()
	tcp := corev1.ProtocolTCP
	udp := corev1.ProtocolUDP
	port53 := intstr.FromInt32(53)
	port443 := intstr.FromInt32(443)
	port8080 := intstr.FromInt32(8080)
	portHub := intstr.FromInt32(cfg.SessiondPort)

	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      NetworkPolicyName,
			Namespace: cfg.Namespace,
			Labels: map[string]string{
				LabelApp:       "pi-sessiond",
				LabelComponent: "sandbox",
			},
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{
				MatchLabels: map[string]string{
					LabelComponent: "sandbox",
				},
			},
			PolicyTypes: []networkingv1.PolicyType{
				networkingv1.PolicyTypeIngress,
				networkingv1.PolicyTypeEgress,
			},
			Ingress: []networkingv1.NetworkPolicyIngressRule{
				{
					From: []networkingv1.NetworkPolicyPeer{
						{
							NamespaceSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{
									"kubernetes.io/metadata.name": cfg.SessionHubNamespace,
								},
							},
							PodSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{
									LabelApp: "session-hub",
								},
							},
						},
					},
					Ports: []networkingv1.NetworkPolicyPort{
						{Protocol: &tcp, Port: &portHub},
					},
				},
			},
			Egress: []networkingv1.NetworkPolicyEgressRule{
				{
					Ports: []networkingv1.NetworkPolicyPort{
						{Protocol: &udp, Port: &port53},
						{Protocol: &tcp, Port: &port53},
					},
				},
				{
					To: []networkingv1.NetworkPolicyPeer{
						{
							NamespaceSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{
									"kubernetes.io/metadata.name": cfg.AIGatewayNamespace,
								},
							},
						},
					},
					Ports: []networkingv1.NetworkPolicyPort{
						{Protocol: &tcp, Port: &port8080},
					},
				},
				{
					To: []networkingv1.NetworkPolicyPeer{
						{
							NamespaceSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{
									"kubernetes.io/metadata.name": cfg.SessionHubNamespace,
								},
							},
							PodSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{
									LabelApp: "session-hub",
								},
							},
						},
					},
					Ports: []networkingv1.NetworkPolicyPort{
						{Protocol: &tcp, Port: &portHub},
					},
				},
				{
					To: []networkingv1.NetworkPolicyPeer{
						{
							IPBlock: &networkingv1.IPBlock{
								CIDR: "0.0.0.0/0",
								Except: []string{
									"169.254.169.254/32", // Block GKE metadata / Workload Identity endpoint
									"10.0.0.0/8",         // Block cluster/pod lateral RFC1918 traffic
									"172.16.0.0/12",
									"192.168.0.0/16",
								},
							},
						},
					},
					Ports: []networkingv1.NetworkPolicyPort{
						{Protocol: &tcp, Port: &port443},
					},
				},
			},
		},
	}
}

// BuildSandboxPod constructs the 3-container interactive sandbox Pod:
//   - Native sidecar `egress-proxy` (Squid allowlist)
//   - Native sidecar `github-auth` (author write-branch App, omitted for analyst sessions)
//   - Main container `agent` running `pi-sessiond` with zero GCS/GCP IAM and AutomountServiceAccountToken=false.
func BuildSandboxPod(cfg PodConfig, spec hub.SpawnSpec) *corev1.Pod {
	cfg = cfg.WithDefaults()
	kind := spec.SessionKind
	if kind == "" {
		kind = sessions.KindInteractive
	}
	tc := spec.Toolchain
	if tc == "" {
		tc = "agent"
	}

	proxyAddr := fmt.Sprintf("http://127.0.0.1:%d", cfg.ProxyPort)
	noProxyHosts := []string{"localhost", "127.0.0.1", "::1"}
	for _, raw := range []string{cfg.GatewayEndpoint, spec.HubURL} {
		if h := urlHost(raw); h != "" {
			noProxyHosts = append(noProxyHosts, h)
		}
	}
	noProxy := strings.Join(noProxyHosts, ",")

	tokenSecret := TokenSecretName(spec.SessionID)
	agentEnv := []corev1.EnvVar{
		{Name: "HTTP_PROXY", Value: proxyAddr},
		{Name: "HTTPS_PROXY", Value: proxyAddr},
		{Name: "NO_PROXY", Value: noProxy},
		{Name: "http_proxy", Value: proxyAddr},
		{Name: "https_proxy", Value: proxyAddr},
		{Name: "no_proxy", Value: noProxy},
		{Name: "SESSION_ID", Value: spec.SessionID},
		{Name: "SESSION_NAME", Value: spec.SessionName},
		{Name: "SESSION_KIND", Value: string(kind)},
		{Name: "TOOLCHAIN", Value: tc},
		{Name: "GITHUB_REPO", Value: spec.Repo},
		{Name: "GITHUB_BRANCH", Value: spec.Branch},
		{Name: "SESSION_HUB_URL", Value: spec.HubURL},
		{
			Name: "SESSION_HUB_TOKEN",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: tokenSecret},
					Key:                  "token",
				},
			},
		},
		{Name: "PI_SESSIOND_ADDR", Value: fmt.Sprintf("0.0.0.0:%d", cfg.SessiondPort)},
		{Name: "PI_WORK_DIR", Value: "/work/repo"},
		{Name: "AGENT_GATEWAY_URL", Value: cfg.GatewayEndpoint},
		{Name: "PI_IDLE_PAUSE_SECONDS", Value: fmt.Sprintf("%d", cfg.IdlePauseSeconds)},
		{Name: "PI_FLUSH_INTERVAL_SECONDS", Value: fmt.Sprintf("%d", cfg.FlushIntervalSeconds)},
	}
	if kind == sessions.KindAnalyst {
		agentEnv = append(agentEnv,
			corev1.EnvVar{Name: "PI_AGENTS_MD", Value: "/opt/agents/analyst.md"},
			corev1.EnvVar{Name: "SESSION_ANALYST_URL", Value: spec.HubURL},
			corev1.EnvVar{
				Name: "SESSION_ANALYST_TOKEN",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: tokenSecret},
						Key:                  "analyst-token",
					},
				},
			},
		)
	}
	if spec.Provider != "" {
		agentEnv = append(agentEnv, corev1.EnvVar{Name: "PI_PROVIDER", Value: spec.Provider})
	}
	if spec.Model != "" {
		agentEnv = append(agentEnv, corev1.EnvVar{Name: "PI_MODEL", Value: spec.Model})
	}
	if cfg.GatewaySecretName != "" {
		agentEnv = append(agentEnv, corev1.EnvVar{
			Name: "AGENT_GATEWAY_TOKEN",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: cfg.GatewaySecretName},
					Key:                  cfg.GatewaySecretKey,
				},
			},
		})
	}

	volumes := []corev1.Volume{
		{Name: "work", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		{Name: "gh-token", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory}}},
		{Name: "egress-config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: EgressConfigMapName}}}},
	}
	agentMounts := []corev1.VolumeMount{
		{Name: "work", MountPath: "/work/repo"},
		{Name: "gh-token", MountPath: "/tokens", ReadOnly: true},
	}
	if cfg.ModelCatalogConfigMap != "" {
		volumes = append(volumes, corev1.Volume{
			Name: "model-catalog",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: cfg.ModelCatalogConfigMap},
				},
			},
		})
		agentMounts = append(agentMounts, corev1.VolumeMount{
			Name:      "model-catalog",
			MountPath: "/etc/pi-sessiond",
			ReadOnly:  true,
		})
		agentEnv = append(agentEnv, corev1.EnvVar{Name: "PI_MODEL_CATALOG", Value: "/etc/pi-sessiond/models.json"})
	}

	always := corev1.ContainerRestartPolicyAlways
	initContainers := []corev1.Container{
		{
			Name:            "egress-proxy",
			Image:           cfg.ProxyImage,
			ImagePullPolicy: cfg.ImagePullPolicy,
			RestartPolicy:   &always,
			Command:         []string{"squid", "-N", "-f", "/etc/squid/conf.d/squid.conf"},
			VolumeMounts: []corev1.VolumeMount{
				{Name: "egress-config", MountPath: "/etc/squid/conf.d", ReadOnly: true},
			},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("100m"),
					corev1.ResourceMemory: resource.MustParse("128Mi"),
				},
			},
		},
	}

	// Include github-auth sidecar for interactive sessions when configured;
	// analyst sessions have zero repo write access and omit github-auth.
	if kind != sessions.KindAnalyst && cfg.GitHubAppSecretName != "" {
		volumes = append(volumes, corev1.Volume{
			Name: "gh-app-key",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: cfg.GitHubAppSecretName},
			},
		})
		ghEnv := []corev1.EnvVar{
			{Name: "GITHUB_APP_PRIVATE_KEY_PATH", Value: "/run/secrets/gh-app/private-key.pem"},
		}
		if cfg.GitHubAppConfigMapName != "" {
			ghEnv = append(ghEnv,
				corev1.EnvVar{
					Name: "GITHUB_APP_ID",
					ValueFrom: &corev1.EnvVarSource{
						ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: cfg.GitHubAppConfigMapName},
							Key:                  "github_app_id",
						},
					},
				},
				corev1.EnvVar{
					Name: "GITHUB_APP_INSTALLATION_ID",
					ValueFrom: &corev1.EnvVarSource{
						ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: cfg.GitHubAppConfigMapName},
							Key:                  "github_app_installation_id",
						},
					},
				},
			)
		}
		if spec.Repo != "" {
			ghEnv = append(ghEnv, corev1.EnvVar{Name: "GITHUB_REPO_NAME", Value: spec.Repo})
		}
		initContainers = append(initContainers, corev1.Container{
			Name:            "github-auth",
			Image:           cfg.GitHubAuthImage,
			ImagePullPolicy: cfg.ImagePullPolicy,
			RestartPolicy:   &always,
			Env:             ghEnv,
			VolumeMounts: []corev1.VolumeMount{
				{Name: "gh-token", MountPath: "/tokens"},
				{Name: "gh-app-key", MountPath: "/run/secrets/gh-app", ReadOnly: true},
			},
			ReadinessProbe: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					Exec: &corev1.ExecAction{Command: []string{"test", "-s", "/tokens/token"}},
				},
				InitialDelaySeconds: 1,
				PeriodSeconds:       2,
			},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("50m"),
					corev1.ResourceMemory: resource.MustParse("64Mi"),
				},
			},
		})
	}

	automount := false
	fsGroup := int64(1000)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      PodName(spec.SessionID),
			Namespace: cfg.Namespace,
			Labels: map[string]string{
				LabelApp:       "pi-sessiond",
				LabelComponent: "sandbox",
				LabelSessionID: spec.SessionID,
				LabelKind:      string(kind),
				LabelToolchain: tc,
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy:                corev1.RestartPolicyNever,
			AutomountServiceAccountToken: &automount,
			ServiceAccountName:           cfg.ServiceAccountName,
			SecurityContext:              &corev1.PodSecurityContext{FSGroup: &fsGroup},
			InitContainers:               initContainers,
			Containers: []corev1.Container{
				{
					Name:            "agent",
					Image:           cfg.SandboxImage(tc),
					ImagePullPolicy: cfg.ImagePullPolicy,
					Ports: []corev1.ContainerPort{
						{Name: "http", ContainerPort: cfg.SessiondPort, Protocol: corev1.ProtocolTCP},
					},
					Env:          agentEnv,
					VolumeMounts: agentMounts,
					ReadinessProbe: &corev1.Probe{
						ProbeHandler: corev1.ProbeHandler{
							HTTPGet: &corev1.HTTPGetAction{
								Path: "/api/health",
								Port: intstr.FromInt32(cfg.SessiondPort),
							},
						},
						InitialDelaySeconds: 1,
						PeriodSeconds:       2,
					},
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("500m"),
							corev1.ResourceMemory: resource.MustParse("1Gi"),
						},
					},
				},
			},
			Volumes: volumes,
		},
	}
	return pod
}

func urlHost(raw string) string {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(u.Hostname()))
}
