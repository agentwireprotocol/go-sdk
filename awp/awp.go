// Package awp is the Go SDK for the Agent Wire Protocol (AWP): a Peer that
// listens and connects, sends messages in threads, and delivers what
// arrives as typed events. It is built on the engine of the reference
// implementation (github.com/agentwireprotocol/awp), so a Peer behaves
// exactly like the awp daemon: resume with an outbox on disk, acks, dedup
// by id, blobs in chunks, grants and introductions, ping/pong, and
// reconnection with backoff.
//
//	p, err := awp.New(awp.Options{Dir: "/var/lib/mybot", Name: "mybot@host", Listen: []string{"tailcat"}})
//	key, err := p.Connect(ctx, "tc...")
//	sent, err := p.Send(awp.Draft{To: key, Subject: "Run the suite", Text: "Please run make test."})
//	for ev := range p.Events(ctx) {
//		if m, ok := ev.(awp.Message); ok { ... }
//	}
package awp

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/agentwireprotocol/awp/node"
	"github.com/agentwireprotocol/awp/store"
	"github.com/agentwireprotocol/awp/wire"
)

// Options configure a Peer. Zero values take the defaults the spec
// recommends.
type Options struct {
	// Dir is the state directory: the identity key, the SQLite store with
	// the outbox, threads and grants, and received blobs. It is created if
	// missing. Empty means a temporary directory removed on Close, with a
	// fresh identity every time: fine for tests and short-lived tools, not
	// for an agent that peers should find again.
	Dir string

	// Name and About are sent in hello. Name defaults to "awp@<hostname>".
	Name  string
	About string

	// Listen lists the addresses to accept connections on: "tailcat" (a
	// WireGuard tunnel with an address peers can reach from anywhere),
	// "tcp:host:port" or "unix:/path". A Peer that only connects out may
	// leave it empty.
	Listen []string

	// Advertise is the address sent in hello so peers can reconnect to
	// this Peer. Empty means the tailcat address if there is one, else the
	// first listen address; "none" sends nothing.
	Advertise string

	// Policy is the admission and capability policy: who may connect,
	// whose grants are honored, and which requests (exec, fs:read,
	// fs:write) the Peer serves by itself.
	Policy Policy

	// BlobLimit caps a blob sent or received; default 50 MiB.
	BlobLimit int64
	// PingInterval is the idle time before a ping; default 30 s. Two
	// missed pongs mean a dead connection.
	PingInterval time.Duration
	// HandshakeTimeout bounds hello and auth; default 30 s.
	HandshakeTimeout time.Duration
	// MaxBackoff caps the reconnect backoff; default 60 s.
	MaxBackoff time.Duration
	// OutboxTTL is how long unacked messages are kept; default 7 days.
	OutboxTTL time.Duration

	// AllowPlaintext permits plain TCP to public addresses, which the spec
	// advises against: TCP has no encryption of its own.
	AllowPlaintext bool
	// Presence publishes a signed summary of this Peer's threads and peers
	// to the network (the presence extension), so dashboards can show it.
	Presence bool

	// Logf receives the engine's log; nil discards it.
	Logf func(format string, args ...any)
	// Trace logs every line sent and received through Logf.
	Trace bool
}

// Policy is the local trust configuration: see the fields of node.Policy.
type Policy = node.Policy

// Admission policies for Policy.Accept.
const (
	AcceptAny       = node.AcceptAny
	AcceptAllowlist = node.AcceptAllowlist
)

// Capabilities with a meaning to the engine (section 10.1 of the spec).
const (
	CapExec      = node.CapExec
	CapFSRead    = node.CapFSRead
	CapFSWrite   = node.CapFSWrite
	CapIntroduce = node.CapIntroduce
	CapAdmin     = node.CapAdmin
)

// Thread states the spec recommends (section 8.1).
const (
	StateOpen    = wire.StateOpen
	StateWorking = wire.StateWorking
	StateWaiting = wire.StateWaiting
	StateDone    = wire.StateDone
	StateFailed  = wire.StateFailed
	StateClosed  = wire.StateClosed
)

// Peer is a running AWP peer.
type Peer struct {
	n         *node.Node
	ephemeral string // a temporary Dir to remove on Close
}

// New opens the state directory, loads or creates the identity, and starts
// listening and reconnecting. Close it when done.
func New(o Options) (*Peer, error) {
	p := &Peer{}
	dir := o.Dir
	if dir == "" {
		tmp, err := os.MkdirTemp("", "awp-")
		if err != nil {
			return nil, err
		}
		dir, p.ephemeral = tmp, tmp
	}
	n, err := node.Open(node.Config{
		Home:             dir,
		Name:             o.Name,
		About:            o.About,
		Listen:           o.Listen,
		Advertise:        o.Advertise,
		Policy:           o.Policy,
		BlobLimit:        o.BlobLimit,
		PingInterval:     o.PingInterval,
		HandshakeTimeout: o.HandshakeTimeout,
		MaxBackoff:       o.MaxBackoff,
		OutboxTTL:        o.OutboxTTL,
		AllowPlaintext:   o.AllowPlaintext,
		Presence:         o.Presence,
		Logf:             o.Logf,
		Trace:            o.Trace,
	})
	if err != nil {
		p.cleanup()
		return nil, err
	}
	p.n = n
	if err := n.Start(); err != nil {
		n.Close()
		p.cleanup()
		return nil, err
	}
	return p, nil
}

func (p *Peer) cleanup() {
	if p.ephemeral != "" {
		os.RemoveAll(p.ephemeral)
	}
}

// Close stops listening, drops every connection without a bye (peers
// resume on the next connection) and closes the store.
func (p *Peer) Close() error {
	err := p.n.Close()
	p.cleanup()
	return err
}

// Key is this Peer's identity: "ed25519:" and the public key.
func (p *Peer) Key() string { return p.n.Key() }

// Name is what this Peer sends in hello.
func (p *Peer) Name() string { return p.n.Name() }

// Dir is the state directory.
func (p *Peer) Dir() string { return p.n.Home() }

// Addresses lists the addresses this Peer listens on, the tailcat one
// first. A tailcat listener takes a few seconds to come up; until then it
// is missing from the list (see TailcatAddress).
func (p *Peer) Addresses() []string { return p.n.Addresses() }

// TailcatAddress is the bare tailcat address, or "" and why not (the
// listener is still starting, or failed).
func (p *Peer) TailcatAddress() (addr, status string) { return p.n.TailcatAddress() }

// Node is the engine underneath, for what this package does not expose.
// Its API follows the reference implementation, not this SDK's versioning.
func (p *Peer) Node() *node.Node { return p.n }

// Connect dials an address shared out of band and returns the key of the
// peer that answered, once the handshake is done. The engine keeps the
// connection and reconnects with backoff while there is unfinished
// business with that peer.
func (p *Peer) Connect(ctx context.Context, addr string) (string, error) {
	return p.n.Connect(ctx, addr)
}

// Connected reports whether the peer has a live connection right now.
func (p *Peer) Connected(peer string) bool { return p.n.Connected(peer) }

// Draft is a message to send.
type Draft struct {
	// To is the peer's key. The peer must be known: connected now, or
	// connected before, or introduced.
	To string
	// Thread continues an existing thread; empty starts a new one.
	Thread string
	// Subject titles a new thread. Empty derives it from the text.
	Subject string
	// ReplyTo names the message this one answers, when it answers one in
	// particular.
	ReplyTo string
	// Text becomes the first part, a text part, when not empty.
	Text string
	// Parts are further parts: text, code or data. Blob parts are made
	// from Files.
	Parts []wire.Part
	// Files are attached as blobs, sent in chunks ahead of the message.
	Files []string
}

// Sent describes a queued message.
type Sent struct {
	ID        string
	Thread    string
	To        string
	NewThread bool
	// Connected says the peer had a live connection when the message was
	// queued. Either way the message is on disk and goes out on the next
	// resume; Delivered waits for the ack.
	Connected bool
}

// Send queues a message. It does not fail because the peer is away: the
// message is written to the outbox and delivered when the peer is back.
func (p *Peer) Send(d Draft) (*Sent, error) {
	parts := d.Parts
	if d.Text != "" {
		parts = append([]wire.Part{{K: wire.PartText, Text: d.Text}}, parts...)
	}
	res, err := p.n.Send(node.SendRequest{Peer: d.To, Th: d.Thread, Subject: d.Subject, Re: d.ReplyTo, Parts: parts, Files: d.Files})
	if err != nil {
		return nil, err
	}
	return &Sent{ID: res.ID, Thread: res.Th, To: res.Peer, NewThread: res.NewThread, Connected: res.Connected}, nil
}

// Delivered waits until the peer has acked the message with this id, or
// ctx ends.
func (p *Peer) Delivered(ctx context.Context, id string) error {
	for {
		ch := p.n.Changed()
		acked, err := p.acked(id)
		if err != nil {
			return err
		}
		if acked {
			return nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (p *Peer) acked(id string) (bool, error) {
	var acked bool
	err := p.n.Store().DB().QueryRow(`SELECT acked FROM log WHERE dir = 'out' AND id = ?`, id).Scan(&acked)
	if errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("no message with id %s", id)
	}
	return acked, err
}

// SetState sends this Peer's view of a thread's state (section 8.1): one
// of the State constants, or any word the two agents agree on. The note
// says more, such as why a thread failed.
func (p *Peer) SetState(peer, thread, state, note string) (*Sent, error) {
	res, err := p.n.SetState(peer, thread, state, note)
	if err != nil {
		return nil, err
	}
	return &Sent{ID: res.ID, Thread: res.Th, To: res.Peer, Connected: res.Connected}, nil
}

// Grant is a capability grant this Peer issued or holds.
type Grant struct {
	// Hash names the grant, for Revoke.
	Hash    string
	Issuer  string
	Subject string
	Caps    []string
	Expires time.Time
	// Audience, when set, is the only peer meant to honor the grant: an
	// introduction's grant.
	Audience string
	// Raw is the signed object as it travels.
	Raw json.RawMessage
}

func grantFrom(g *store.GrantRow) *Grant {
	return &Grant{Hash: g.Hash, Issuer: g.Iss, Subject: g.Sub, Caps: g.Caps, Expires: g.Exp, Audience: g.Audience, Raw: g.Raw}
}

// Grant mints a grant giving the peer the capabilities until ttl passes,
// records it, and sends it (now, or after the next resume).
func (p *Peer) Grant(peer string, caps []string, ttl time.Duration) (*Grant, error) {
	g, err := p.n.Grant(peer, caps, ttl)
	if err != nil {
		return nil, err
	}
	return grantFrom(g), nil
}

// Revoke stops honoring a grant this Peer issued. The peer's copy stays
// valid elsewhere until it expires.
func (p *Peer) Revoke(hash string) error {
	k, err := p.n.Revoke(hash)
	if err != nil {
		return err
	}
	if k == 0 {
		return fmt.Errorf("no grant %s", hash)
	}
	return nil
}

// Grants lists the grants this Peer issued and holds, valid now.
func (p *Peer) Grants() (issued, held []*Grant, err error) {
	now := time.Now()
	is, err := p.n.Store().Grants(store.GrantQuery{Role: store.RoleIssued, ValidAt: now})
	if err != nil {
		return nil, nil, err
	}
	hs, err := p.n.Store().Grants(store.GrantQuery{Role: store.RoleHeld, ValidAt: now})
	if err != nil {
		return nil, nil, err
	}
	for _, g := range is {
		issued = append(issued, grantFrom(g))
	}
	for _, g := range hs {
		held = append(held, grantFrom(g))
	}
	return issued, held, nil
}

// Caps lists the capabilities the peer holds on this Peer beyond the
// defaults, from the grants this Peer honors.
func (p *Peer) Caps(peer string) []string { return p.n.Caps(peer) }

// Introduce hands peer `to` the identity and address of `peer`, with a
// grant for the capabilities that `peer` honors if it trusts this Peer
// with introduce (section 10.4). With a thread the introduction is queued
// like a message; without one it needs a live connection to `to`.
func (p *Peer) Introduce(to, peer string, caps []string, ttl time.Duration, thread string) (*Sent, error) {
	res, err := p.n.Introduce(to, peer, caps, ttl, thread)
	if err != nil {
		return nil, err
	}
	return &Sent{ID: res.ID, Thread: res.Th, To: res.Peer, Connected: res.Connected}, nil
}

// Bye closes the connection to the peer gracefully and parks it: no
// reconnection until something new is queued for it.
func (p *Peer) Bye(peer, reason string) error { return p.n.Bye(peer, reason) }

// PeerInfo is what this Peer knows about another.
type PeerInfo struct {
	Key       string
	Name      string
	About     string
	Caps      []string
	Addrs     []string
	Connected bool
	LastSeen  time.Time
}

// Peers lists every peer this Peer has met.
func (p *Peer) Peers() ([]PeerInfo, error) {
	rows, err := p.n.Store().Peers()
	if err != nil {
		return nil, err
	}
	out := make([]PeerInfo, 0, len(rows))
	for _, r := range rows {
		info := PeerInfo{Key: r.Key, Name: r.Name, About: r.About, Caps: r.Caps, Connected: p.n.Connected(r.Key), LastSeen: r.LastSeen}
		for _, a := range r.Addrs {
			info.Addrs = append(info.Addrs, a.Addr)
		}
		out = append(out, info)
	}
	return out, nil
}

// Resolve turns a key, a name or a unique prefix of a key into the key.
func (p *Peer) Resolve(s string) (string, error) { return p.n.ResolvePeer(s) }

// Thread is a conversation with one peer about one thing, with each side's
// state.
type Thread struct {
	Peer       string
	ID         string
	Subject    string
	Created    time.Time
	Updated    time.Time
	MyState    string
	MyNote     string
	TheirState string
	TheirNote  string
	Unread     int
}

func threadFrom(t *store.Thread) Thread {
	return Thread{Peer: t.Peer, ID: t.Th, Subject: t.Subject, Created: t.Created, Updated: t.Updated,
		MyState: t.MyState, MyNote: t.MyNote, TheirState: t.TheirState, TheirNote: t.TheirNote, Unread: t.Unread}
}

// Threads lists the threads with a peer, or with everyone when peer is "".
func (p *Peer) Threads(peer string) ([]Thread, error) {
	rows, err := p.n.Store().Threads(peer, "")
	if err != nil {
		return nil, err
	}
	out := make([]Thread, 0, len(rows))
	for _, r := range rows {
		out = append(out, threadFrom(r))
	}
	return out, nil
}

// Thread returns one thread, or nil if there is none.
func (p *Peer) Thread(peer, id string) (*Thread, error) {
	t, err := store.GetThread(p.n.Store().DB(), peer, id)
	if err != nil || t == nil {
		return nil, err
	}
	th := threadFrom(t)
	return &th, nil
}

// History returns a thread's events in order, both directions: messages
// and states sent and received, blobs, errors.
func (p *Peer) History(peer, thread string) ([]Event, error) {
	recs, err := p.n.Store().Query(store.Filter{Peer: peer, Th: thread, Limit: 100000})
	if err != nil {
		return nil, err
	}
	var out []Event
	for i := range recs {
		if ev := eventFrom(&recs[i]); ev != nil {
			out = append(out, ev)
		}
	}
	return out, nil
}

// MarkRead marks the thread's received messages as read, so Unread counts
// and the Unread field go down. Reading is a local notion; nothing is sent.
func (p *Peer) MarkRead(peer, thread string) error {
	recs, err := p.n.Store().Query(store.Filter{Peer: peer, Th: thread, Dirs: []string{"in"}, UnreadOnly: true, Limit: 100000})
	if err != nil {
		return err
	}
	seqs := make([]int64, 0, len(recs))
	for _, r := range recs {
		seqs = append(seqs, r.Seq)
	}
	if len(seqs) == 0 {
		return nil
	}
	return p.n.Store().MarkRead(seqs)
}

// Changed returns a channel closed at the next state change: a message in
// or out, an ack, a connection coming or going. Waiters check their
// condition and call Changed again. Events is the typed view of the same.
func (p *Peer) Changed() <-chan struct{} { return p.n.Changed() }
