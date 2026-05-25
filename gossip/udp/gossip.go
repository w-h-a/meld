// Package udp implements gossip transport over UDP.
package udp

import (
	"context"
	"errors"
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
	peers     []*net.UDPAddr
	msgCh     chan []byte
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
func (g *udpGossip) Listen(_ context.Context) (<-chan []byte, error) {
	if !g.listening.CompareAndSwap(false, true) {
		return nil, errors.New("already listening")
	}

	g.msgCh = make(chan []byte, 64)

	g.wg.Add(1)
	go g.readUDP()

	return g.msgCh, nil
}

// Broadcast sends msg to all known peers via UDP. Returns an
// error if the message exceeds the MTU, or if every peer fails.
func (g *udpGossip) Broadcast(ctx context.Context, msg []byte) error {
	ctx, span := g.tracer.Start(ctx, "Gossip.Broadcast",
		trace.WithAttributes(
			attribute.String("gossip.direction", "send"),
			attribute.String("gossip.transport", "udp"),
			attribute.Int("gossip.message_bytes", len(msg)),
		),
	)
	defer span.End()

	if len(msg) >= g.mtu {
		return errors.New("message exceeds MTU")
	}

	var lastErr error
	sent := 0

	for _, addr := range g.peers {
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

	_, span := g.tracer.Start(g.options.Context, "Gossip.Listen",
		trace.WithAttributes(
			attribute.String("gossip.direction", "recv"),
			attribute.String("gossip.transport", "udp"),
			attribute.String("gossip.bind_address", g.udpConn.LocalAddr().String()),
		),
	)
	defer span.End()

	buf := make([]byte, 65535)

	for {
		n, _, err := g.udpConn.ReadFromUDP(buf)
		if err != nil {
			select {
			case <-g.done:
				return
			default:
				span.AddEvent("udp.read_error", trace.WithAttributes(attribute.String("error.message", err.Error())))
				continue
			}
		}

		msg := make([]byte, n)
		copy(msg, buf[:n])

		select {
		case g.msgCh <- msg:
		case <-g.done:
			return
		}
	}
}
