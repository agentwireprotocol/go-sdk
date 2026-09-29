package awp

import (
	"context"
	"encoding/json"
	"time"

	"github.com/agentwireprotocol/awp/node"
	"github.com/agentwireprotocol/awp/store"
	"github.com/agentwireprotocol/awp/wire"
)

// Event is something that happened: a message or state from a peer, a
// blob that arrived, a connection that came or went, a grant, an
// introduction, an error a peer sent, a bye. Switch on the concrete type.
type Event interface {
	// PeerKey is the peer the event is about.
	PeerKey() string
	// Time is when it happened.
	Time() time.Time
}

type base struct {
	Peer string
	At   time.Time
}

func (b base) PeerKey() string { return b.Peer }
func (b base) Time() time.Time { return b.At }

// Connected: a handshake with the peer completed.
type Connected struct {
	base
	Name     string
	About    string
	Caps     []string
	Via      string
	Outbound bool
}

// Disconnected: the connection to the peer ended. The engine reconnects
// with backoff while there is unfinished business with the peer.
type Disconnected struct {
	base
	Reason string
	Via    string
}

// Refused: a peer was turned away by the admission policy.
type Refused struct {
	base
	Name   string
	Reason string
}

// Message: a msg from the peer, durably stored and acked before this
// event is delivered.
type Message struct {
	base
	ID      string
	Thread  string
	Subject string
	ReplyTo string
	Parts   []wire.Part
	// Requests lists capability requests found in the data parts (exec,
	// fs:read, fs:write, admin), each marked allowed by the sender's grants
	// or not. A denied request was answered with err forbidden already;
	// an allowed one is for this Peer to act on, unless Policy.Serve
	// served it.
	Requests []Request
	// Sent is true for messages this Peer sent, in History.
	Sent bool
}

// Request is a capability request in a message.
type Request = node.Request

// Text joins the message's text parts, for the common case.
func (m Message) Text() string {
	var out string
	for _, p := range m.Parts {
		if p.K == wire.PartText {
			if out != "" {
				out += "\n"
			}
			out += p.Text
		}
	}
	return out
}

// StateChanged: the peer set its state on a thread.
type StateChanged struct {
	base
	ID     string
	Thread string
	State  string
	Note   string
	// Sent is true for states this Peer set, in History.
	Sent bool
}

// Blob: a file the peer sent arrived in full, at Path.
type Blob struct {
	base
	Thread string
	Ref    string
	Name   string
	Mime   string
	Size   int64
	Path   string
}

// GrantReceived: the peer gave this Peer a grant, now held and presented
// to its issuer on every connection.
type GrantReceived struct {
	base
	Issuer  string
	Caps    []string
	Expires time.Time
}

// Introduced: the peer handed this Peer another peer's key and address,
// and a grant that peer honors if it trusts the introducer. Connect to
// Address to meet it.
type Introduced struct {
	base
	Thread  string
	Key     string
	Name    string
	Address string
}

// PeerError: the peer sent an err line. Code is one of the spec's codes.
type PeerError struct {
	base
	Code   string
	Detail string
	// ReplyTo is the id of the message the error answers, if any.
	ReplyTo string
	// Ref names the refused blob for blob_refused.
	Ref string
}

// Bye: the peer closed gracefully. It is parked: no reconnection until
// something new is queued for it.
type Bye struct {
	base
	Reason string
}

// Events delivers every event from now on, in order, until ctx ends. The
// channel is closed then. Slow consumers hold events back but lose none.
func (p *Peer) Events(ctx context.Context) <-chan Event {
	out := make(chan Event, 64)
	seq, _ := p.n.Store().MaxSeq()
	go func() {
		defer close(out)
		for {
			ch := p.n.Changed()
			recs, err := p.n.Store().Query(store.Filter{AfterSeq: seq, Limit: 1000})
			if err == nil {
				for i := range recs {
					seq = recs[i].Seq
					ev := eventFrom(&recs[i])
					if ev == nil {
						continue
					}
					select {
					case out <- ev:
					case <-ctx.Done():
						return
					}
				}
				if len(recs) == 1000 {
					continue // there may be more
				}
			}
			select {
			case <-ch:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// eventFrom translates a stored record into an event, or nil for records
// with no event of their own (acks, pings, chunks, outgoing grants).
func eventFrom(r *store.Record) Event {
	b := base{Peer: r.Peer, At: r.At}
	str := func(k string) string {
		s, _ := r.Meta[k].(string)
		return s
	}
	switch r.Dir {
	case "in", "out":
		sent := r.Dir == "out"
		switch r.T {
		case wire.TMsg:
			var m wire.Msg
			if err := wire.Decode(r.Line, &m); err != nil {
				return nil
			}
			ev := Message{base: b, ID: m.ID, Thread: m.Th, Subject: m.Subject, ReplyTo: m.Re, Parts: m.Parts, Sent: sent}
			if raw, ok := r.Meta["requests"]; ok {
				if bs, err := json.Marshal(raw); err == nil {
					json.Unmarshal(bs, &ev.Requests)
				}
			}
			return ev
		case wire.TState:
			var s wire.State
			if err := wire.Decode(r.Line, &s); err != nil {
				return nil
			}
			return StateChanged{base: b, ID: s.ID, Thread: s.Th, State: s.State, Note: s.Note, Sent: sent}
		}
		if sent {
			return nil
		}
		switch r.T {
		case wire.TErr:
			return PeerError{base: b, Code: str("code"), Detail: str("detail"), ReplyTo: str("re"), Ref: str("ref")}
		case wire.TGrant:
			var caps []string
			if cs, ok := r.Meta["caps"].([]any); ok {
				for _, c := range cs {
					if s, ok := c.(string); ok {
						caps = append(caps, s)
					}
				}
			}
			exp, _ := time.Parse(time.RFC3339Nano, str("exp"))
			return GrantReceived{base: b, Issuer: str("iss"), Caps: caps, Expires: exp}
		case wire.TIntroduce:
			return Introduced{base: b, Thread: r.Th, Key: str("key"), Name: str("name"), Address: str("address")}
		}
	case "sys":
		switch r.T {
		case "connected":
			ev := Connected{base: b, Name: str("name"), About: str("about"), Via: str("via")}
			ev.Outbound, _ = r.Meta["outbound"].(bool)
			if cs, ok := r.Meta["caps"].([]any); ok {
				for _, c := range cs {
					if s, ok := c.(string); ok {
						ev.Caps = append(ev.Caps, s)
					}
				}
			}
			return ev
		case "disconnected":
			return Disconnected{base: b, Reason: str("reason"), Via: str("via")}
		case "refused":
			return Refused{base: b, Name: str("name"), Reason: str("reason")}
		case "blob":
			size, _ := r.Meta["size"].(float64)
			if n, ok := r.Meta["size"].(int64); ok {
				size = float64(n)
			}
			return Blob{base: b, Thread: str("th"), Ref: str("ref"), Name: str("name"), Mime: str("mime"), Size: int64(size), Path: str("path")}
		case "bye":
			if str("from") == "peer" {
				return Bye{base: b, Reason: str("reason")}
			}
		}
	}
	return nil
}
