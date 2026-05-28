// Package udp implements gossip transport over UDP.
package udp

import (
	"context"
	"errors"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"

	"github.com/w-h-a/meld/gossip"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type udpGossip struct {
	options   gossip.Options
	mtu       int
	udpConn   *net.UDPConn
	peerMtx   sync.RWMutex
	peers     []*net.UDPAddr
	msgCh     chan *gossip.Packet
	done      chan struct{}
	wg        sync.WaitGroup
	listening atomic.Bool
	once      sync.Once
	tracer    trace.Tracer
}

func New(opts ...gossip.Option) (gossip.Gossip, error) {
	options := gossip.NewOptions(opts...)

	udpAddr, err := net.ResolveUDPAddr("udp", options.BindAddress)
	if err != nil {
		return nil, err
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}

	peers := make([]*net.UDPAddr, 0, len(options.Peers))
	for _, p := range options.Peers {
		addr, err := net.ResolveUDPAddr("udp", p)
		if err != nil {
			conn.Close()
			return nil, err
		}
		peers = append(peers, addr)
	}

	g := &udpGossip{
		options: options,
		mtu:     mtuFrom(options.Context),
		udpConn: conn,
		peers:   peers,
		done:    make(chan struct{}),
		tracer:  otel.Tracer("meld/gossip/udp"),
	}

	return g, nil
}

// Addr returns the bound address of the UDP socket.
func (g *udpGossip) Addr(ctx context.Context) net.Addr {
	return g.udpConn.LocalAddr()
}

// Listen starts a routine reading UDP datagrams and returns
// a channel of incoming messages. Must be called at most once.
// The channel is closed when Stop is called.
func (g *udpGossip) Listen(_ context.Context) (<-chan *gossip.Packet, error) {
	if !g.listening.CompareAndSwap(false, true) {
		return nil, errors.New("already listening")
	}

	g.msgCh = make(chan *gossip.Packet, 64)

	g.wg.Add(1)
	go g.readUDP()

	return g.msgCh, nil
}

// Broadcast sends msg to all known peers via UDP.
func (g *udpGossip) Broadcast(ctx context.Context, msg []byte) error {
	ctx, span := g.tracer.Start(ctx, "Gossip.Broadcast",
		trace.WithAttributes(
			attribute.String("gossip.direction", "send"),
			attribute.String("gossip.transport", "udp"),
			attribute.Int("gossip.message_bytes", len(msg)),
			attribute.Int("gossip.fanout", g.options.Fanout),
		),
	)
	defer span.End()

	if len(msg) >= g.mtu {
		return errors.New("message exceeds MTU")
	}

	g.peerMtx.RLock()
	snapshot := make([]*net.UDPAddr, len(g.peers))
	copy(snapshot, g.peers)
	g.peerMtx.RUnlock()

	span.SetAttributes(attribute.Int("gossip.peers_available", len(snapshot)))

	n := min(g.options.Fanout, len(snapshot))

	for i := range n {
		j := i + rand.IntN(len(snapshot)-i)
		snapshot[i], snapshot[j] = snapshot[j], snapshot[i]
	}
	targets := snapshot[:n]

	span.SetAttributes(attribute.Int("gossip.peers_selected", n))

	var lastErr error
	sent := 0

	for _, addr := range targets {
		if err := ctx.Err(); err != nil {
			return err
		}

		if _, err := g.udpConn.WriteTo(msg, addr); err != nil {
			lastErr = err
			span.RecordError(err, trace.WithAttributes(attribute.String("gossip.peer_address", addr.String())))
			continue
		}

		sent++
	}

	span.SetAttributes(attribute.Int("gossip.peers_sent", sent))

	if sent == 0 && lastErr != nil {
		span.SetStatus(codes.Error, lastErr.Error())
		return lastErr
	}

	return nil
}

// SetPeers replaces the peer list.
func (g *udpGossip) SetPeers(ctx context.Context, peers ...string) error {
	_, span := g.tracer.Start(ctx, "Gossip.SetPeers",
		trace.WithAttributes(
			attribute.String("gossip.transport", "udp"),
			attribute.Int("gossip.peers_count", len(peers)),
		),
	)
	defer span.End()

	resolved := make([]*net.UDPAddr, 0, len(peers))
	for _, p := range peers {
		addr, err := net.ResolveUDPAddr("udp", p)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
		resolved = append(resolved, addr)
	}

	g.peerMtx.Lock()
	g.peers = resolved
	g.peerMtx.Unlock()

	return nil
}

// Stop closes the UDP socket, waits for the read routine to
// exit, and closes the message channel. Safe to call multiple
// times.
func (g *udpGossip) Stop(_ context.Context) error {
	g.once.Do(func() {
		close(g.done)
		g.udpConn.Close()
		g.wg.Wait()
		if g.msgCh != nil {
			close(g.msgCh)
		}
	})
	return nil
}

// readUDP loops reading datagrams from the socket and forwarding
// them to the message channel.
func (g *udpGossip) readUDP() {
	defer g.wg.Done()

	ctx, span := g.tracer.Start(g.options.Context, "Gossip.Listen",
		trace.WithAttributes(
			attribute.String("gossip.direction", "recv"),
			attribute.String("gossip.transport", "udp"),
			attribute.String("gossip.bind_address", g.udpConn.LocalAddr().String()),
		),
	)
	defer span.End()

	buf := make([]byte, 65535)

	for {
		n, addr, err := g.udpConn.ReadFromUDP(buf)
		if err != nil {
			select {
			case <-g.done:
				return
			default:
				span.AddEvent("udp.read_error", trace.WithAttributes(attribute.String("error.message", err.Error())))
				continue
			}
		}

		if !g.handleDatagram(ctx, addr, buf[:n]) {
			return
		}
	}
}

// handleDatagram processes a single received datagram.
func (g *udpGossip) handleDatagram(ctx context.Context, addr *net.UDPAddr, data []byte) bool {
	_, span := g.tracer.Start(ctx, "Gossip.Receive", trace.WithAttributes(
		attribute.String("gossip.sender_address", addr.String()),
		attribute.String("gossip.transport", "udp"),
		attribute.Int("gossip.message_bytes", len(data)),
	))
	defer span.End()

	msg := make([]byte, len(data))
	copy(msg, data)

	select {
	case <-g.done:
		return false
	case g.msgCh <- &gossip.Packet{From: addr, Data: msg}:
		return true
	}
}
