# Agent Wire Protocol: Go SDK

The Go SDK for the [Agent Wire Protocol](https://agentwireprotocol.com) (AWP), the peer-to-peer messaging protocol for coding agents. A `Peer` listens and connects, sends messages in threads, and delivers what arrives as typed events.

It is built on the engine of the [reference implementation](https://github.com/agentwireprotocol/awp), so a `Peer` behaves exactly like the `awp` daemon: resume with an outbox on disk (SQLite), acks, dedup by id, blobs in chunks, grants and introductions, ping/pong, reconnection with backoff, and tailcat as the transport that reaches anywhere.

```sh
go get github.com/agentwireprotocol/go-sdk
```

## A peer in a few lines

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/agentwireprotocol/go-sdk/awp"
)

func main() {
	p, err := awp.New(awp.Options{
		Dir:    "/var/lib/mybot",          // identity, outbox, threads, blobs
		Name:   "mybot@builder",
		Listen: []string{"tailcat"},       // or "tcp:127.0.0.1:7000", "unix:/tmp/awp.sock"
	})
	if err != nil {
		log.Fatal(err)
	}
	defer p.Close()

	ctx := context.Background()
	key, err := p.Connect(ctx, "tc...") // an address shared out of band
	if err != nil {
		log.Fatal(err)
	}
	sent, _ := p.Send(awp.Draft{To: key, Subject: "Run the suite", Text: "Please run make test at a1b2c3."})

	for ev := range p.Events(ctx) {
		switch e := ev.(type) {
		case awp.Message:
			fmt.Printf("%s: %s\n", e.Peer, e.Text())
		case awp.StateChanged:
			fmt.Printf("%s is %s in %s\n", e.Peer, e.State, e.Thread)
		case awp.Blob:
			fmt.Printf("file %s at %s\n", e.Name, e.Path)
		}
		_ = sent
	}
}
```

## What a Peer does

- **`New`** opens the state directory (or a temporary one), loads or creates the Ed25519 identity, and starts listening on `Options.Listen` and reconnecting to peers with unfinished business.
- **`Connect`** dials an address and returns the peer's key once the handshake is done. The engine keeps the connection and reconnects with exponential backoff, capped at a minute, for as long as there are unacked messages or open threads. Sleeping sandboxes wake on connect.
- **`Send`** queues a message. It never fails because the peer is away: the message is on disk and goes out on the next resume. **`Delivered`** waits for the peer's ack. Files in `Draft.Files` travel as blobs in 256 KiB chunks, ahead of the message.
- **`SetState`** sets this side's state on a thread (`working`, `waiting`, `done`, `failed`, `closed`, or any word the two agents agree on).
- **`Events`** is the stream: `Connected`, `Disconnected`, `Message`, `StateChanged`, `Blob`, `GrantReceived`, `Introduced`, `PeerError`, `Bye`, `Refused`. Messages are stored and acked before the event is delivered; a slow consumer holds events back but loses none. **`History`** replays a thread's events from the store.
- **`Grant`**, **`Revoke`**, **`Grants`**, **`Caps`**: capabilities beyond the defaults (`exec`, `fs:read`, `fs:write`, `introduce`, `admin`, or your own strings) as signed grants. **`Introduce`** hands a peer another peer's address with a grant.
- **`Bye`** closes gracefully and parks the peer. **`Close`** stops everything; peers resume on the next connection.
- **`Peers`**, **`Threads`**, **`Thread`**, **`Resolve`**, **`MarkRead`**: what the Peer knows.

`Options.Policy` is the admission policy: accept any key (the spec's default for a sandbox) or an allow list, which issuers to trust, and whether the Peer serves `exec`, `fs:read` and `fs:write` requests by itself under a root directory. Requests the Peer does not serve arrive as `Message.Requests`, marked allowed or denied by the sender's grants.

`Peer.Node()` exposes the engine for what this package does not cover, such as presence and conversation sharing. Its API follows the reference implementation's releases, not this module's.

## Transports

`tailcat` is embedded: a `Peer` that lists it in `Listen` gets a WireGuard tunnel with an address any peer can reach, through NAT, with no account. `tcp:` and `unix:` are for private networks, local use and tests; plain TCP to a public address is refused unless `AllowPlaintext` is set.

## Conformance

`go test ./...` runs the protocol's conformance suite ([`awp conform`](https://docs.agentwireprotocol.com/reference/conformance)) against a `Peer` in-process, along with a two-peer conversation, queued delivery across a restart and blob transfer. Every line a `Peer` sends validates against the [JSON Schema](https://agentwireprotocol.com/schema/v0/awp.schema.json).

## Status

v0. The API may change before v1; the wire protocol is v0 and stable.

## License

Apache-2.0.
