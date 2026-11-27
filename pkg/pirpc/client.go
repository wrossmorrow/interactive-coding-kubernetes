// Package pirpc manages a long-lived `pi --mode rpc` child process and fans
// session events out to subscribers.
//
// The wire protocol is JSONL over the child's stdin/stdout (see
// https://pi.dev/docs/latest/rpc): one JSON object per line. Commands are
// written to stdin with an `id`; responses echo the id and carry
// `{success, error?, data?}`. Everything else on stdout is a session event
// (or an `extension_ui_request` awaiting an `extension_ui_response`).
package pirpc

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os/exec"
	"sync"
	"time"
)

const (
	// DefaultCommandTimeout bounds how long a command may take to produce its
	// correlated response.
	DefaultCommandTimeout = 30 * time.Second
	// subscriberBuffer is how many events a subscriber may fall behind before
	// it is disconnected. A disconnected browser reconnects and resyncs from
	// get_messages, which is cheaper and more correct than silently dropping
	// streaming deltas.
	subscriberBuffer = 4096
	// maxLineBytes bounds a single JSONL record from the child.
	maxLineBytes = 64 * 1024 * 1024
)

// CommandResult is the response to an RPC command. Success is false when
// the command was rejected; Error carries the failure message. Data is kept
// raw so fields pi omits (sessionName, contextUsage, ...) pass through as-is.
type CommandResult struct {
	Success bool            `json:"success"`
	Error   string          `json:"error,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// Event is one session event from the child, forwarded to browsers verbatim
// (modulo slimming, see SlimEvent).
type Event = map[string]any

// Client manages a long-lived `pi --mode rpc` child process and fans session
// events out to subscribers.
type Client struct {
	bin  string
	args []string
	env  []string
	dir  string

	writeMu sync.Mutex
	stdin   io.WriteCloser
	cmd     *exec.Cmd

	mu        sync.Mutex
	dead      bool
	streaming bool

	pendingMu sync.Mutex
	pending   map[string]chan CommandResult
	seq       int64

	// subsMu guards subscribers and the replay state below, so a new
	// subscriber gets a consistent snapshot followed by every later event.
	subsMu  sync.Mutex
	subs    map[chan Event]struct{}
	dialogs map[string]Event // open dialog requests, by id
	order   []string         // dialog ids in arrival order
	ui      map[string]Event // latest setStatus/setWidget/setTitle, by key
	queue   Event            // latest queue_update, replayed to new subscribers

	exitCh chan error
}

// NewClient returns a client for the pi binary at bin invoked with args using
// env and working directory dir (empty inherits current directory).
func NewClient(bin string, args []string, env []string, dir string) *Client {
	return &Client{
		bin:     bin,
		args:    args,
		env:     env,
		dir:     dir,
		pending: map[string]chan CommandResult{},
		subs:    map[chan Event]struct{}{},
		dialogs: map[string]Event{},
		ui:      map[string]Event{},
		queue:   Event{"type": ""},
		exitCh:  make(chan error, 1),
	}
}

// Start spawns the pi RPC child process.
func (c *Client) Start() error {
	cmd := exec.Command(c.bin, c.args...)
	cmd.Env = c.env
	if c.dir != "" {
		cmd.Dir = c.dir
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	c.cmd = cmd
	c.stdin = stdin

	go func() {
		sc := bufio.NewScanner(stderr)
		sc.Buffer(make([]byte, 64*1024), 1024*1024)
		for sc.Scan() {
			log.Printf("pi: %s", sc.Text())
		}
	}()
	go func() {
		c.readLoop(stdout)
		err := cmd.Wait()
		c.handleExit(err)
		c.exitCh <- err
	}()
	return nil
}

// Wait blocks until the child process exits and returns its exit error.
func (c *Client) Wait() error { return <-c.exitCh }

// Stop closes stdin and terminates the child process if it does not exit promptly.
func (c *Client) Stop() error {
	c.writeMu.Lock()
	if c.stdin != nil {
		_ = c.stdin.Close()
		c.stdin = nil
	}
	c.writeMu.Unlock()
	if c.cmd == nil || c.cmd.Process == nil {
		return nil
	}
	return c.cmd.Process.Kill()
}

// IsDead reports whether the child process has exited.
func (c *Client) IsDead() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dead
}

// IsStreaming reports whether the agent is currently executing a turn.
func (c *Client) IsStreaming() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.streaming
}

// SubscriberCount reports the number of currently attached event subscribers.
func (c *Client) SubscriberCount() int {
	c.subsMu.Lock()
	defer c.subsMu.Unlock()
	return len(c.subs)
}

// Broadcast sends a synthetic event (such as session_paused or session_closed)
// to all live subscribers.
func (c *Client) Broadcast(ev Event) {
	c.broadcast(ev)
}

// handleExit marks the client dead, fails outstanding commands, and closes
// subscriber channels so HTTP handlers and SSE clients wind down.
func (c *Client) handleExit(err error) {
	c.mu.Lock()
	c.dead = true
	c.streaming = false
	c.mu.Unlock()

	msg := "pi rpc process exited"
	if err != nil {
		msg += ": " + err.Error()
	}
	c.pendingMu.Lock()
	pending := c.pending
	c.pending = map[string]chan CommandResult{}
	c.pendingMu.Unlock()
	for _, ch := range pending {
		ch <- CommandResult{Success: false, Error: msg}
	}

	c.subsMu.Lock()
	for ch := range c.subs {
		delete(c.subs, ch)
		close(ch)
	}
	c.subsMu.Unlock()
}

// readLoop consumes stdout JSONL records until EOF.
func (c *Client) readLoop(r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), maxLineBytes)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var raw struct {
			Type    string          `json:"type"`
			ID      string          `json:"id"`
			Success bool            `json:"success"`
			Error   string          `json:"error"`
			Data    json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(line, &raw); err != nil {
			continue
		}
		if raw.Type == "response" {
			c.resolve(raw.ID, CommandResult{Success: raw.Success, Error: raw.Error, Data: raw.Data})
			continue
		}
		var ev Event
		if err := json.Unmarshal(line, &ev); err != nil {
			continue
		}
		c.trackStreaming(ev)
		c.broadcast(SlimEvent(ev))
	}
	if err := sc.Err(); err != nil {
		log.Printf("pi rpc read loop stopped: %v", err)
	}
}

func (c *Client) trackStreaming(ev Event) {
	switch ev["type"] {
	case "agent_start":
		c.mu.Lock()
		c.streaming = true
		c.mu.Unlock()
	case "agent_end", "agent_settled":
		c.mu.Lock()
		c.streaming = false
		c.mu.Unlock()
	}
}

func (c *Client) resolve(id string, res CommandResult) {
	c.pendingMu.Lock()
	ch, ok := c.pending[id]
	delete(c.pending, id)
	c.pendingMu.Unlock()
	if ok {
		ch <- res
	}
}

// SlimEvent drops the cumulative `message`/`partial` snapshots pi attaches to
// every message_update. For toolcall_* updates the block being streamed is
// kept (as `block`) so the browser knows the tool name and call id early.
func SlimEvent(ev Event) Event {
	if ev["type"] != "message_update" {
		return ev
	}
	delete(ev, "message")
	ame, ok := ev["assistantMessageEvent"].(map[string]any)
	if !ok {
		return ev
	}
	if partial, ok := ame["partial"].(map[string]any); ok {
		if idx, ok := ame["contentIndex"].(float64); ok {
			if content, ok := partial["content"].([]any); ok && int(idx) < len(content) && int(idx) >= 0 {
				if block, ok := content[int(idx)].(map[string]any); ok && block["type"] == "toolCall" {
					ame["block"] = block
				}
			}
		}
		delete(ame, "partial")
	}
	return ev
}

// broadcast records replayable UI state and fans the event out.
func (c *Client) broadcast(ev Event) {
	c.subsMu.Lock()
	defer c.subsMu.Unlock()
	c.remember(ev)
	for ch := range c.subs {
		select {
		case ch <- ev:
		default:
			delete(c.subs, ch)
			close(ch)
		}
	}
}

// remember tracks state a freshly (re)connected browser needs that it cannot
// get from any RPC command: open dialogs, extension status/widget/title, and
// the latest steering/follow-up queue.
func (c *Client) remember(ev Event) {
	if ev["type"] == "queue_update" {
		c.queue = ev
		return
	}
	if ev["type"] != "extension_ui_request" {
		return
	}
	id, _ := ev["id"].(string)
	method, _ := ev["method"].(string)
	switch method {
	case "select", "confirm", "input", "editor":
		if _, ok := c.dialogs[id]; !ok {
			c.order = append(c.order, id)
		}
		c.dialogs[id] = ev
		if ms, ok := ev["timeout"].(float64); ok && ms > 0 {
			time.AfterFunc(time.Duration(ms)*time.Millisecond, func() { c.forgetDialog(id) })
		}
	case "setStatus":
		key, _ := ev["statusKey"].(string)
		c.rememberUI("status:"+key, ev, ev["statusText"] == nil)
	case "setWidget":
		key, _ := ev["widgetKey"].(string)
		c.rememberUI("widget:"+key, ev, ev["widgetLines"] == nil)
	case "setTitle":
		c.rememberUI("title", ev, false)
	}
}

func (c *Client) rememberUI(key string, ev Event, clear bool) {
	if clear {
		delete(c.ui, key)
	} else {
		c.ui[key] = ev
	}
}

// forgetDialog drops a dialog once it is answered and notifies subscribers.
func (c *Client) forgetDialog(id string) {
	c.subsMu.Lock()
	_, ok := c.dialogs[id]
	delete(c.dialogs, id)
	for i, v := range c.order {
		if v == id {
			c.order = append(c.order[:i], c.order[i+1:]...)
			break
		}
	}
	c.subsMu.Unlock()
	if ok {
		c.broadcast(Event{"type": "web_dialog_closed", "id": id})
	}
}

// Subscribe registers a new event channel, preloaded with the replay state.
func (c *Client) Subscribe() (<-chan Event, func()) {
	ch := make(chan Event, subscriberBuffer)
	c.subsMu.Lock()
	for _, ev := range c.ui {
		ch <- ev
	}
	for _, id := range c.order {
		ch <- c.dialogs[id]
	}
	if c.queue["type"] != "" {
		ch <- c.queue
	}
	if c.IsDead() {
		close(ch)
	} else {
		c.subs[ch] = struct{}{}
	}
	c.subsMu.Unlock()
	return ch, func() {
		c.subsMu.Lock()
		if _, ok := c.subs[ch]; ok {
			delete(c.subs, ch)
			close(ch)
		}
		c.subsMu.Unlock()
	}
}

// Send writes one command (with a client-generated correlation id) and waits
// for its response.
func (c *Client) Send(ctx context.Context, command map[string]any) (*CommandResult, error) {
	if c.IsDead() {
		return nil, fmt.Errorf("pi rpc process is not running")
	}
	c.pendingMu.Lock()
	c.seq++
	id := fmt.Sprintf("rpc_%d", c.seq)
	ch := make(chan CommandResult, 1)
	c.pending[id] = ch
	c.pendingMu.Unlock()

	full := make(map[string]any, len(command)+1)
	for k, v := range command {
		full[k] = v
	}
	full["id"] = id
	if err := c.writeJSON(full); err != nil {
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()
		return nil, err
	}
	select {
	case res := <-ch:
		return &res, nil
	case <-ctx.Done():
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()
		return nil, ctx.Err()
	}
}

// State sends the get_state command and returns the response data payload.
func (c *Client) State(ctx context.Context) (map[string]any, error) {
	res, err := c.Send(ctx, map[string]any{"type": "get_state"})
	if err != nil {
		return nil, err
	}
	if !res.Success {
		return nil, fmt.Errorf("get_state failed: %s", res.Error)
	}
	var data map[string]any
	if len(res.Data) > 0 {
		if err := json.Unmarshal(res.Data, &data); err != nil {
			return nil, fmt.Errorf("get_state: bad data: %w", err)
		}
	}
	return data, nil
}

// Stats sends the get_session_stats command and returns the response data.
func (c *Client) Stats(ctx context.Context) (map[string]any, error) {
	res, err := c.Send(ctx, map[string]any{"type": "get_session_stats"})
	if err != nil {
		return nil, err
	}
	if !res.Success {
		return nil, fmt.Errorf("get_session_stats failed: %s", res.Error)
	}
	var data map[string]any
	if len(res.Data) > 0 {
		if err := json.Unmarshal(res.Data, &data); err != nil {
			return nil, fmt.Errorf("get_session_stats: bad data: %w", err)
		}
	}
	return data, nil
}

// DialogOpen reports whether a dialog is currently pending for id.
func (c *Client) DialogOpen(id string) bool {
	c.subsMu.Lock()
	defer c.subsMu.Unlock()
	_, ok := c.dialogs[id]
	return ok
}

// RespondDialog answers an open extension UI dialog.
func (c *Client) RespondDialog(response map[string]any) error {
	id, _ := response["id"].(string)
	response["type"] = "extension_ui_response"
	if err := c.writeJSON(response); err != nil {
		return err
	}
	c.forgetDialog(id)
	return nil
}

func (c *Client) writeJSON(v any) error {
	line, err := json.Marshal(v)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.stdin == nil {
		return fmt.Errorf("pi rpc stdin is not open")
	}
	if _, err := c.stdin.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("writing to pi rpc stdin: %w", err)
	}
	return nil
}
