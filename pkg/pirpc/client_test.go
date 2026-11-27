package pirpc

import (
	"strings"
	"testing"
)

func TestSlimEventDropsSnapshotsKeepsToolBlock(t *testing.T) {
	ev := Event{
		"type":    "message_update",
		"message": map[string]any{"role": "assistant"},
		"assistantMessageEvent": map[string]any{
			"type":         "toolcall_start",
			"contentIndex": float64(1),
			"partial": map[string]any{"content": []any{
				map[string]any{"type": "text", "text": "hi"},
				map[string]any{"type": "toolCall", "id": "call_1", "name": "bash"},
			}},
		},
	}
	out := SlimEvent(ev)
	if _, ok := out["message"]; ok {
		t.Fatal("message snapshot not dropped")
	}
	ame := out["assistantMessageEvent"].(map[string]any)
	if _, ok := ame["partial"]; ok {
		t.Fatal("partial snapshot not dropped")
	}
	block, ok := ame["block"].(map[string]any)
	if !ok || block["id"] != "call_1" {
		t.Fatalf("tool block not kept: %v", ame["block"])
	}

	other := Event{"type": "tool_execution_update", "partialResult": map[string]any{}}
	if _, ok := SlimEvent(other)["partialResult"]; !ok {
		t.Fatal("non message_update events must pass through untouched")
	}
}

func TestRememberReplaysOpenDialogsAndStatus(t *testing.T) {
	c := NewClient("", nil, nil, "")
	c.broadcast(Event{"type": "extension_ui_request", "id": "s", "method": "setStatus", "statusKey": "k", "statusText": "v"})
	c.broadcast(Event{"type": "extension_ui_request", "id": "d1", "method": "confirm", "title": "ok?"})
	c.broadcast(Event{"type": "extension_ui_request", "id": "d2", "method": "select", "options": []any{"a"}})
	c.forgetDialog("d1")

	ch, unsub := c.Subscribe()
	defer unsub()
	var got []string
	for len(ch) > 0 {
		ev := <-ch
		got = append(got, ev["id"].(string))
	}
	if strings.Join(got, ",") != "s,d2" {
		t.Fatalf("replay = %v, want [s d2]", got)
	}

	c.broadcast(Event{"type": "extension_ui_request", "id": "s2", "method": "setStatus", "statusKey": "k"})
	ch2, unsub2 := c.Subscribe()
	defer unsub2()
	if n := len(ch2); n != 1 {
		t.Fatalf("after clearing status, replay has %d events, want 1", n)
	}
}

func TestRememberReplaysQueueUpdate(t *testing.T) {
	c := NewClient("", nil, nil, "")
	c.broadcast(Event{"type": "queue_update", "steering": []any{"steer"}, "followUp": []any{"next"}})
	ch, unsub := c.Subscribe()
	defer unsub()
	ev := <-ch
	if ev["type"] != "queue_update" {
		t.Fatalf("replayed event = %v, want queue_update", ev)
	}
}

func TestHandleExitClosesSubscribersWithoutClosingSession(t *testing.T) {
	c := NewClient("", nil, nil, "")
	ch, unsub := c.Subscribe()
	defer unsub()
	c.handleExit(nil)
	if _, ok := <-ch; ok {
		t.Fatal("expected subscriber channel to be closed on handleExit")
	}
	if !c.IsDead() {
		t.Fatal("expected client to be marked dead")
	}
}
