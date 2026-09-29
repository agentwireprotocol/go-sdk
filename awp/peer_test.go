package awp_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/agentwireprotocol/awp/conformance"
	"github.com/agentwireprotocol/go-sdk/awp"
)

func newPeer(t *testing.T, name string) *awp.Peer {
	t.Helper()
	dir := t.TempDir()
	p, err := awp.New(awp.Options{
		Dir:          dir,
		Name:         name,
		Listen:       []string{"unix:" + filepath.Join(dir, "s")},
		PingInterval: 2 * time.Second,
		MaxBackoff:   500 * time.Millisecond,
		Logf:         func(f string, a ...any) { t.Logf("["+name+"] "+f, a...) },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

// next returns the first event of type T, skipping others, within 15s.
func next[T awp.Event](t *testing.T, events <-chan awp.Event) T {
	t.Helper()
	deadline := time.After(15 * time.Second)
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatal("events closed")
			}
			if v, ok := ev.(T); ok {
				return v
			}
		case <-deadline:
			var zero T
			t.Fatalf("no %T within 15s", zero)
		}
	}
}

func TestConversation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	a, b := newPeer(t, "a@test"), newPeer(t, "b@test")
	aEvents, bEvents := a.Events(ctx), b.Events(ctx)

	key, err := a.Connect(ctx, b.Addresses()[0])
	if err != nil {
		t.Fatal(err)
	}
	if key != b.Key() {
		t.Fatalf("connected to %s, want %s", key, b.Key())
	}
	if c := next[awp.Connected](t, bEvents); c.Peer != a.Key() || c.Name != "a@test" || c.Outbound {
		t.Fatalf("b's connected event: %+v", c)
	}
	if c := next[awp.Connected](t, aEvents); c.Peer != b.Key() || !c.Outbound {
		t.Fatalf("a's connected event: %+v", c)
	}

	// A message with text and a data part starts a thread; b sees it.
	sent, err := a.Send(awp.Draft{To: b.Key(), Subject: "Run the suite", Text: "Please run make test."})
	if err != nil {
		t.Fatal(err)
	}
	if !sent.NewThread || sent.Thread == "" {
		t.Fatalf("sent: %+v", sent)
	}
	m := next[awp.Message](t, bEvents)
	if m.Peer != a.Key() || m.Thread != sent.Thread || m.Subject != "Run the suite" || m.Text() != "Please run make test." || m.Sent {
		t.Fatalf("message: %+v", m)
	}
	if err := a.Delivered(ctx, sent.ID); err != nil {
		t.Fatal(err)
	}

	// b works, replies in the thread, and finishes.
	if _, err := b.SetState(a.Key(), m.Thread, awp.StateWorking, "running"); err != nil {
		t.Fatal(err)
	}
	if s := next[awp.StateChanged](t, aEvents); s.Thread != m.Thread || s.State != awp.StateWorking || s.Note != "running" {
		t.Fatalf("state: %+v", s)
	}
	reply, err := b.Send(awp.Draft{To: a.Key(), Thread: m.Thread, ReplyTo: m.ID, Text: "3 of 42 failing"})
	if err != nil {
		t.Fatal(err)
	}
	if r := next[awp.Message](t, aEvents); r.ID != reply.ID || r.ReplyTo != m.ID || r.Text() != "3 of 42 failing" {
		t.Fatalf("reply: %+v", r)
	}
	th, err := a.Thread(b.Key(), m.Thread)
	if err != nil || th == nil {
		t.Fatalf("thread: %v %v", th, err)
	}
	// Unread counts what b sent and a has not read: the state and the reply.
	if th.TheirState != awp.StateWorking || th.Subject != "Run the suite" || th.Unread != 2 {
		t.Fatalf("thread: %+v", th)
	}
	if err := a.MarkRead(b.Key(), m.Thread); err != nil {
		t.Fatal(err)
	}
	if th, _ = a.Thread(b.Key(), m.Thread); th.Unread != 0 {
		t.Fatalf("unread after MarkRead: %d", th.Unread)
	}
	hist, err := a.History(b.Key(), m.Thread)
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 3 {
		t.Fatalf("history has %d events, want 3: %+v", len(hist), hist)
	}
	if first, ok := hist[0].(awp.Message); !ok || !first.Sent || first.ID != sent.ID {
		t.Fatalf("history[0]: %+v", hist[0])
	}

	// A file goes as a blob and arrives byte for byte.
	path := filepath.Join(t.TempDir(), "log.txt")
	data := bytes.Repeat([]byte("integration output\n"), 20000) // several chunks
	os.WriteFile(path, data, 0o600)
	if _, err := a.Send(awp.Draft{To: b.Key(), Thread: m.Thread, Text: "log attached", Files: []string{path}}); err != nil {
		t.Fatal(err)
	}
	blob := next[awp.Blob](t, bEvents)
	if blob.Name != "log.txt" || blob.Size != int64(len(data)) || blob.Thread != m.Thread {
		t.Fatalf("blob: %+v", blob)
	}
	if got, _ := os.ReadFile(blob.Path); !bytes.Equal(got, data) {
		t.Fatalf("blob content differs (%d bytes)", len(got))
	}

	// A grant from a to b: b holds it, a lists it as issued.
	g, err := a.Grant(b.Key(), []string{awp.CapFSRead}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if gr := next[awp.GrantReceived](t, bEvents); gr.Issuer != a.Key() || len(gr.Caps) != 1 || gr.Caps[0] != awp.CapFSRead {
		t.Fatalf("grant received: %+v", gr)
	}
	issued, _, err := a.Grants()
	if err != nil || len(issued) != 1 || issued[0].Hash != g.Hash {
		t.Fatalf("grants: %v %v", issued, err)
	}
	if caps := a.Caps(b.Key()); len(caps) != 1 || caps[0] != awp.CapFSRead {
		t.Fatalf("caps: %v", caps)
	}
	if err := a.Revoke(g.Hash); err != nil {
		t.Fatal(err)
	}
	if caps := a.Caps(b.Key()); len(caps) != 0 {
		t.Fatalf("caps after revoke: %v", caps)
	}

	peers, err := a.Peers()
	if err != nil || len(peers) != 1 || peers[0].Key != b.Key() || !peers[0].Connected || peers[0].Name != "b@test" {
		t.Fatalf("peers: %+v %v", peers, err)
	}

	// Bye from b: a sees it and the disconnect.
	if err := b.Bye(a.Key(), "done"); err != nil {
		t.Fatal(err)
	}
	if by := next[awp.Bye](t, aEvents); by.Reason != "done" {
		t.Fatalf("bye: %+v", by)
	}
	next[awp.Disconnected](t, aEvents)
	if a.Connected(b.Key()) {
		t.Fatal("still connected after bye")
	}
}

// TestQueuedWhileAway: a message to a peer that is down waits in the
// outbox and arrives when the peer is back, once.
func TestQueuedWhileAway(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	a := newPeer(t, "a@test")
	bDir := t.TempDir()
	open := func() *awp.Peer {
		p, err := awp.New(awp.Options{Dir: bDir, Name: "b@test", Listen: []string{"unix:" + filepath.Join(bDir, "s")}, PingInterval: 2 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	b := open()
	aEvents := a.Events(ctx)
	bKey, err := a.Connect(ctx, b.Addresses()[0])
	if err != nil {
		t.Fatal(err)
	}
	next[awp.Connected](t, aEvents)
	b.Close()
	next[awp.Disconnected](t, aEvents)

	sent, err := a.Send(awp.Draft{To: bKey, Text: "while you were out"})
	if err != nil {
		t.Fatal(err)
	}
	if sent.Connected {
		t.Fatal("reported connected while b is down")
	}
	b = open()
	defer b.Close()
	bEvents := b.Events(ctx)
	if err := a.Delivered(ctx, sent.ID); err != nil {
		t.Fatal(err)
	}
	hist, _ := b.History(a.Key(), sent.Thread)
	if len(hist) != 1 {
		t.Fatalf("b has %d events in the thread, want 1", len(hist))
	}
	_ = bEvents
}

// TestConformance runs the protocol conformance suite against a Peer.
func TestConformance(t *testing.T) {
	p := newPeer(t, "sdk@test")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	rep, err := conformance.Run(ctx, conformance.Options{Addr: p.Addresses()[0], Timeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rep.Results {
		if r.Status != "pass" {
			t.Errorf("%s: %s %s", r.Name, r.Status, r.Reason)
		}
	}
	if rep.Passed != len(conformance.Scenarios()) {
		t.Fatalf("%d passed of %d", rep.Passed, len(conformance.Scenarios()))
	}
}

func TestEphemeral(t *testing.T) {
	p, err := awp.New(awp.Options{Name: "tmp@test"})
	if err != nil {
		t.Fatal(err)
	}
	dir := p.Dir()
	if _, err := os.Stat(dir); err != nil {
		t.Fatal(err)
	}
	if p.Key() == "" || len(p.Addresses()) != 0 {
		t.Fatalf("key %q addresses %v", p.Key(), p.Addresses())
	}
	p.Close()
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("temporary dir %s still exists", dir)
	}
}
