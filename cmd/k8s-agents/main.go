// Command k8s-agents provides developer CLI commands for the Kubernetes pi
// interactive session workbench, including zero-ingress local UI tunneling over
// kubeconfig (`k8s-agents ui`).
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "ui":
		runUI(os.Args[2:])
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `Usage: k8s-agents <command> [flags]

Commands:
  ui    Open a zero-ingress local kubeconfig tunnel to svc/session-hub on 127.0.0.1`)
}

func runUI(args []string) {
	fs := flag.NewFlagSet("ui", flag.ExitOnError)
	namespace := fs.String("namespace", "session-system", "Kubernetes namespace running session-hub")
	service := fs.String("service", "session-hub", "Kubernetes Service name for session-hub")
	localPort := fs.Int("port", 8787, "local loopback port on 127.0.0.1")
	remotePort := fs.Int("remote-port", 8787, "remote Service port on session-hub")
	kubeContext := fs.String("context", "", "optional kubectl context name")
	openBrowser := fs.Bool("open", false, "automatically open the UI in the default browser")
	_ = fs.Parse(args)

	if _, err := exec.LookPath("kubectl"); err != nil {
		log.Fatalf("kubectl not found in PATH: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	localURL := fmt.Sprintf("http://127.0.0.1:%d", *localPort)
	log.Printf("tunneling %s -> %s/svc/%s:%d (Ctrl+C to stop)", localURL, *namespace, *service, *remotePort)

	if *openBrowser {
		go func() {
			time.Sleep(1200 * time.Millisecond)
			openURL(localURL)
		}()
	}

	for {
		if ctx.Err() != nil {
			return
		}
		kargs := []string{
			"port-forward",
			"-n", *namespace,
			"--address", "127.0.0.1",
			"svc/" + *service,
			fmt.Sprintf("%d:%d", *localPort, *remotePort),
		}
		if *kubeContext != "" {
			kargs = append([]string{"--context", *kubeContext}, kargs...)
		}

		cmd := exec.CommandContext(ctx, "kubectl", kargs...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		err := cmd.Run()
		if ctx.Err() != nil {
			return
		}
		log.Printf("port-forward disconnected (%v); reconnecting in 2s...", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func openURL(u string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", u)
	case "linux":
		cmd = exec.Command("xdg-open", u)
	default:
		return
	}
	_ = cmd.Start()
}
