// Command echo is a peer that listens and answers every message in its
// thread. Run it, share the address it prints, and send it something:
//
//	go run ./examples/echo
//	awp connect <address> && awp send echo "hello"
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/agentwireprotocol/go-sdk/awp"
)

func main() {
	dir := flag.String("dir", "", "state directory (default: a temporary one, new identity each run)")
	listen := flag.String("listen", "tailcat", "carrier to listen on: tailcat, udp:HOST:PORT, ws:HOST:PORT, cloudflare or unix:/path")
	flag.Parse()

	p, err := awp.New(awp.Options{Dir: *dir, Name: "echo", Listen: []string{*listen}, Logf: log.Printf})
	if err != nil {
		log.Fatal(err)
	}
	defer p.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	fmt.Println("key:", p.Key())
	addr, err := p.WaitAddress(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("address:", addr)
	for ev := range p.Events(ctx) {
		switch e := ev.(type) {
		case awp.Connected:
			fmt.Printf("connected: %s (%s)\n", e.Name, e.Peer)
		case awp.Message:
			fmt.Printf("%s: %s\n", e.Peer, e.Text())
			p.SetState(e.Peer, e.Thread, awp.StateWorking, "")
			p.Send(awp.Draft{To: e.Peer, Thread: e.Thread, ReplyTo: e.ID, Text: "echo: " + strings.TrimSpace(e.Text())})
			p.SetState(e.Peer, e.Thread, awp.StateDone, "")
		case awp.Blob:
			fmt.Printf("received %s (%d bytes) at %s\n", e.Name, e.Size, e.Path)
		case awp.Disconnected:
			fmt.Printf("disconnected: %s (%s)\n", e.Peer, e.Reason)
		}
	}
}
