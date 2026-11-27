// Command session-hub runs the control-plane API, archive viewer, internal
// sandbox ingest endpoint, live /s/<id>/* SSE/RPC proxy, and decoupled SPA.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/wrossmorrow/k8s-agents/pkg/hub"
	"github.com/wrossmorrow/k8s-agents/pkg/k8s"
	"github.com/wrossmorrow/k8s-agents/pkg/piconfig"
	"github.com/wrossmorrow/k8s-agents/pkg/sessions"
	"github.com/wrossmorrow/k8s-agents/ui"
)

func main() {
	var (
		addr             = flag.String("addr", envOr("SESSION_HUB_ADDR", "127.0.0.1:8787"), "HTTP listen address")
		hubURL           = flag.String("hub-url", os.Getenv("SESSION_HUB_URL"), "internal URL passed to spawned sandboxes (defaults to http://<addr>)")
		spawnerMode      = flag.String("spawner", envOr("SESSION_SPAWNER", "local"), "sandbox spawner mode: local or k8s")
		storeDir         = flag.String("store-dir", envOr("SESSION_STORE_DIR", ".dev/store"), "local DirStore path (used when --gcs-bucket is empty)")
		gcsBucket        = flag.String("gcs-bucket", os.Getenv("SESSION_GCS_BUCKET"), "GCS bucket for session storage (uses Workload Identity / ADC)")
		workDir          = flag.String("work-dir", envOr("SESSION_WORK_DIR", ".dev/work"), "local work directory for LocalSpawner")
		sessiondBin      = flag.String("sessiond-bin", envOr("PI_SESSIOND_BIN", "pi-sessiond"), "path to pi-sessiond binary for LocalSpawner")
		piBin            = flag.String("pi-bin", envOr("PI_BIN", "pi"), "pi binary passed to LocalSpawner")
		piArgs           = flag.String("pi-args", os.Getenv("PI_ARGS"), "optional override args for pi")
		modelCatalog     = flag.String("model-catalog", os.Getenv("PI_MODEL_CATALOG"), "optional JSON model catalog path")
		archiveSinkToken = flag.String("archive-sink-token", os.Getenv("SESSION_SINK_TOKEN"), "bearer token accepted on POST /internal/archive for agentrunner CI archives")
		allowedHosts     = flag.String("allowed-hosts", os.Getenv("SESSION_HUB_ALLOWED_HOSTS"), "comma-separated extra allowed Host headers")
		flushIntervalSec = flag.Int("flush-interval", 30, "periodic flush interval (seconds) for spawned daemons")

		// Kubernetes PodSpawner options (--spawner=k8s).
		k8sNamespace      = flag.String("k8s-namespace", envOr("K8S_SANDBOX_NAMESPACE", k8s.DefaultNamespace), "namespace for sandbox Pods")
		k8sImageRepo      = flag.String("k8s-image-repo", envOr("K8S_SANDBOX_IMAGE_REPO", "ghcr.io/wrossmorrow"), "container registry/repo for sandbox images")
		k8sImageTag       = flag.String("k8s-image-tag", envOr("K8S_SANDBOX_IMAGE_TAG", "latest"), "image tag for sandbox images")
		k8sGatewayURL     = flag.String("k8s-gateway-url", envOr("AGENT_GATEWAY_URL", "http://agent-router.ai-gateway.svc.cluster.local:8080/v1"), "Agent Router OpenAI-compatible base URL")
		k8sGatewaySecret  = flag.String("k8s-gateway-secret", os.Getenv("K8S_GATEWAY_SECRET"), "Secret name holding AGENT_GATEWAY_TOKEN in sandbox namespace")
		k8sGitHubSecret   = flag.String("k8s-github-app-secret", os.Getenv("K8S_GITHUB_APP_SECRET"), "Secret name holding interactive author GitHub App PEM")
		k8sGitHubCM       = flag.String("k8s-github-app-configmap", os.Getenv("K8S_GITHUB_APP_CONFIGMAP"), "ConfigMap name holding interactive GitHub App ID and installation ID")
		k8sModelCatalogCM = flag.String("k8s-model-catalog-configmap", envOr("K8S_MODEL_CATALOG_CONFIGMAP", "session-model-catalog"), "ConfigMap name holding models.json catalog in sandbox namespace")
		k8sIdlePauseSec   = flag.Int("k8s-idle-pause-seconds", k8s.DefaultIdlePauseSeconds, "idle auto-pause timeout (seconds) for sandbox Pods")
	)
	flag.Parse()

	var (
		store sessions.Store
		err   error
	)
	if *gcsBucket != "" {
		gcs, gerr := sessions.NewGCSStore(context.Background(), *gcsBucket)
		if gerr != nil {
			log.Fatalf("init gcs store: %v", gerr)
		}
		defer func() { _ = gcs.Close() }()
		store = gcs
	} else {
		store, err = sessions.NewDirStore(*storeDir)
		if err != nil {
			log.Fatalf("init dir store: %v", err)
		}
	}

	cat := piconfig.DefaultCatalog()
	if *modelCatalog != "" {
		loaded, err := piconfig.LoadCatalog(*modelCatalog)
		if err != nil {
			log.Fatalf("load model catalog: %v", err)
		}
		cat = loaded
	}

	var (
		srv         *hub.Server
		spawner     hub.Spawner
		tokenLookup func(string) (string, bool)
	)
	onExit := func(id string) {
		if srv != nil {
			srv.NotifyExit(id)
		}
	}

	switch strings.ToLower(strings.TrimSpace(*spawnerMode)) {
	case "local":
		spawner = hub.NewLocalSpawner(hub.LocalSpawnerConfig{
			SessiondBin:      *sessiondBin,
			WorkRoot:         *workDir,
			PiBin:            *piBin,
			PiArgs:           *piArgs,
			ModelCatalogPath: *modelCatalog,
			FlushIntervalSec: *flushIntervalSec,
			OnExit:           onExit,
		})
	case "k8s":
		clientset, err := buildKubeClient()
		if err != nil {
			log.Fatalf("init kubernetes client: %v", err)
		}
		k8sSpawner := k8s.NewPodSpawner(clientset, k8s.PodConfig{
			Namespace:              *k8sNamespace,
			ImageRepository:        *k8sImageRepo,
			ImageTag:               *k8sImageTag,
			GatewayEndpoint:        *k8sGatewayURL,
			GatewaySecretName:      *k8sGatewaySecret,
			GitHubAppSecretName:    *k8sGitHubSecret,
			GitHubAppConfigMapName: *k8sGitHubCM,
			ModelCatalogConfigMap:  *k8sModelCatalogCM,
			IdlePauseSeconds:       *k8sIdlePauseSec,
			FlushIntervalSeconds:   *flushIntervalSec,
		}, onExit)
		spawner = k8sSpawner
		tokenLookup = k8sSpawner.LookupToken
	default:
		log.Fatalf("unsupported --spawner=%q (want local or k8s)", *spawnerMode)
	}
	defer func() { _ = spawner.CloseAll() }()

	hubBaseURL := strings.TrimSpace(*hubURL)
	if hubBaseURL == "" {
		hubBaseURL = "http://" + *addr
		if strings.HasPrefix(*addr, ":") {
			hubBaseURL = "http://127.0.0.1" + *addr
		}
	}

	srv = hub.NewServer(hub.Config{
		Store:            store,
		Spawner:          spawner,
		Catalog:          cat,
		UIFS:             ui.FS(),
		HubBaseURL:       hubBaseURL,
		ArchiveSinkToken: *archiveSinkToken,
		TokenLookup:      tokenLookup,
		AllowedHosts:     strings.Split(*allowedHosts, ","),
	})

	httpSrv := &http.Server{
		Addr:    *addr,
		Handler: srv.Routes(),
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("session-hub shutting down: %v", sig)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(ctx)
		_ = spawner.CloseAll()
	}()

	log.Printf("session-hub listening on %s (spawner=%s, hub-url=%s)", *addr, *spawnerMode, hubBaseURL)
	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("http server error: %v", err)
	}
}

func buildKubeClient() (kubernetes.Interface, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		rules := clientcmd.NewDefaultClientConfigLoadingRules()
		cfg, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{}).ClientConfig()
		if err != nil {
			return nil, err
		}
	}
	return kubernetes.NewForConfig(cfg)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
