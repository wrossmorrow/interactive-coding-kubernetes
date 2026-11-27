package sessiond

import (
	"context"
	"log"
	"time"
)

// StartIdleWatcher monitors the daemon and calls Pause + shutdown once the
// session has been idle (not streaming and zero connected SSE subscribers)
// continuously for idleTimeout. A non-positive idleTimeout disables watching.
func (s *Server) StartIdleWatcher(ctx context.Context, idleTimeout time.Duration) func() {
	if idleTimeout <= 0 {
		return func() {}
	}
	wctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		checkEvery := idleTimeout / 4
		if checkEvery < time.Second {
			checkEvery = time.Second
		}
		if checkEvery > 30*time.Second {
			checkEvery = 30 * time.Second
		}
		t := time.NewTicker(checkEvery)
		defer t.Stop()
		var idleSince time.Time
		for {
			select {
			case <-wctx.Done():
				return
			case <-t.C:
				if s.rpc == nil || s.rpc.IsDead() {
					return
				}
				if s.rpc.IsStreaming() || s.rpc.SubscriberCount() > 0 {
					idleSince = time.Time{}
					continue
				}
				if idleSince.IsZero() {
					idleSince = time.Now()
					continue
				}
				if time.Since(idleSince) >= idleTimeout {
					log.Printf("session %s idle for %s; auto-pausing", s.cfg.SessionID, idleTimeout)
					pctx, pcancel := context.WithTimeout(context.Background(), 45*time.Second)
					if _, err := s.Pause(pctx); err != nil {
						log.Printf("session %s idle pause error: %v", s.cfg.SessionID, err)
					}
					pcancel()
					if s.onShutdown != nil {
						s.onShutdown()
					}
					return
				}
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}
