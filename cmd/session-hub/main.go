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

	"github.com/wrossmorrow/k8s-agents/pkg/hub"
	"github.com/wrossmorrow/k8s-agents/pkg/piconfig"
	"github.com/wrossmorrow/k8s-agents/pkg/sessions"
	"github.com/wrossmorrow/k8s-agents/ui"
)

func main() {
	var (
		addr             = flag.String("addr", envOr("SESSION_HUB_ADDR", "127.0.0.1:8787"), "HTTP listen address")
		storeDir         = flag.String("store-dir", envOr("SESSION_STORE_DIR", ".dev/store"), "local DirStore path (used when --gcs-bucket is empty)")
		gcsBucket        = flag.String("gcs-bucket", os.Getenv("SESSION_GCS_BUCKET"), "GCS bucket for session storage (uses Workload Identity / ADC)")
		workDir          = flag.String("work-dir", envOr("SESSION_WORK_DIR", ".dev/work"), "local work directory for LocalSpawner")
		sessiondBin      = flag.String("sessiond-bin", envOr("PI_SESSIOND_BIN", "pi-sessiond"), "path to pi-sessiond binary for LocalSpawner")
		piBin            = flag.String("pi-bin", envOr("PI_BIN", "pi"), "pi binary passed to LocalSpawner")
		piArgs           = flag.String("pi-args", os.Getenv("PI_ARGS"), "optional override args for pi")
		modelCatalog     = flag.String("model-catalog", os.Getenv("PI_MODEL_CATALOG"), "optional JSON model catalog path")
		allowedHosts     = flag.String("allowed-hosts", os.Getenv("SESSION_HUB_ALLOWED_HOSTS"), "comma-separated extra allowed Host headers")
		flushIntervalSec = flag.Int("flush-interval", 15, "periodic flush interval (seconds) for spawned daemons")
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

	var srv *hub.Server
	spawner := hub.NewLocalSpawner(hub.LocalSpawnerConfig{
		SessiondBin:      *sessiondBin,
		WorkRoot:         *workDir,
		PiBin:            *piBin,
		PiArgs:           *piArgs,
		ModelCatalogPath: *modelCatalog,
		FlushIntervalSec: *flushIntervalSec,
		OnExit: func(id string) {
			if srv != nil {
				srv.NotifyExit(id)
			}
		},
	})
	defer func() { _ = spawner.CloseAll() }()

	hubBaseURL := "http://" + *addr
	if strings.HasPrefix(*addr, ":") {
		hubBaseURL = "http://127.0.0.1" + *addr
	}
	srv = hub.NewServer(hub.Config{
		Store:        store,
		Spawner:      spawner,
		Catalog:      cat,
		UIFS:         ui.FS(),
		HubBaseURL:   hubBaseURL,
		AllowedHosts: strings.Split(*allowedHosts, ","),
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

	log.Printf("session-hub listening on %s", hubBaseURL)
	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("http server error: %v", err)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
