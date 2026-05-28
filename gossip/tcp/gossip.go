// Package tcp implements gossip transport over TCP with
// length-prefixed framing.
package tcp

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/w-h-a/meld/gossip"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	maxMessageSize  = 16 * 1024 * 1024 // 16 MB
	maxAddrSize     = 256
	connReadTimeout = 30 * time.Second
)

type tcpGossip struct {
	options   gossip.Options
	listener  *net.TCPListener
	peerMtx   sync.RWMutex
	peers     []string
	msgCh     chan *gossip.Packet
	done      chan struct{}
	wg        sync.WaitGroup
	listening atomic.Bool
	once      sync.Once
	connMtx   sync.RWMutex
	conns     map[net.Conn]struct{}
	tracer    trace.Tracer
}

func New(opts ...gossip.Option) (gossip.Gossip, error) {
	options := gossip.NewOptions(opts...)

	tcpAddr, err := net.ResolveTCPAddr("tcp", options.BindAddress)
	if err != nil {
		return nil, err
	}

	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return nil, err
	}

	g := &tcpGossip{
		options:  options,
		listener: listener,
		peers:    options.Peers,
		done:     make(chan struct{}),
		conns:    map[net.Conn]struct{}{},
		tracer:   otel.Tracer("meld/gossip/tcp"),
	}

	return g, nil
}

// Addr returns the bound address of the TCP listener.
func (g *tcpGossip) Addr(_ context.Context) net.Addr {
	return g.listener.Addr()
}

// Listen starts a routine accepting TCP connections and returns
// a channel of incoming messages. Must be called at most once.
// The channel is closed when Stop is called.
func (g *tcpGossip) Listen(_ context.Context) (<-chan *gossip.Packet, error) {
	if !g.listening.CompareAndSwap(false, true) {
		return nil, errors.New("already listening")
	}

	g.msgCh = make(chan *gossip.Packet, 64)

	g.wg.Add(1)
	go g.acceptLoop()

	return g.msgCh, nil
}

// Broadcast sends msg to all known peers via TCP.
func (g *tcpGossip) Broadcast(ctx context.Context, msg []byte) error {
	ctx, span := g.tracer.Start(ctx, "Gossip.Broadcast",
		trace.WithAttributes(
			attribute.String("gossip.direction", "send"),
			attribute.String("gossip.transport", "tcp"),
			attribute.Int("gossip.message_bytes", len(msg)),
			attribute.Int("gossip.fanout", g.options.Fanout),
		),
	)
	defer span.End()

	g.peerMtx.RLock()
	snapshot := make([]string, len(g.peers))
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

		if err := g.sendTo(ctx, addr, msg); err != nil {
			lastErr = err
			span.RecordError(err, trace.WithAttributes(attribute.String("gossip.peer_address", addr)))
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
func (g *tcpGossip) SetPeers(ctx context.Context, peers ...string) error {
	_, span := g.tracer.Start(ctx, "Gossip.SetPeers",
		trace.WithAttributes(
			attribute.String("gossip.transport", "tcp"),
			attribute.Int("gossip.peers_count", len(peers)),
		),
	)
	defer span.End()

	next := make([]string, len(peers))
	copy(next, peers)

	g.peerMtx.Lock()
	g.peers = next
	g.peerMtx.Unlock()

	return nil
}

// Stop closes the TCP listener, waits for all connection
// handlers to exit, and closes the message channel. Safe to
// call multiple times.
func (g *tcpGossip) Stop(_ context.Context) error {
	g.once.Do(func() {
		close(g.done)
		g.listener.Close()
		g.connMtx.RLock()
		for conn := range g.conns {
			conn.Close()
		}
		g.connMtx.RUnlock()
		g.wg.Wait()
		if g.msgCh != nil {
			close(g.msgCh)
		}
	})
	return nil
}

func (g *tcpGossip) sendTo(ctx context.Context, addr string, msg []byte) error {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	if deadline, ok := ctx.Deadline(); ok {
		conn.SetWriteDeadline(deadline)
	}

	advAddr := []byte(g.listener.Addr().String())

	addrHeader := make([]byte, 4)
	binary.BigEndian.PutUint32(addrHeader, uint32(len(advAddr)))
	if _, err := conn.Write(addrHeader); err != nil {
		return err
	}
	if _, err := conn.Write(advAddr); err != nil {
		return err
	}

	payloadHeader := make([]byte, 4)
	binary.BigEndian.PutUint32(payloadHeader, uint32(len(msg)))
	if _, err := conn.Write(payloadHeader); err != nil {
		return err
	}
	if _, err := conn.Write(msg); err != nil {
		return err
	}

	return nil
}

// acceptLoop accepts inbound TCP connections and spawns a
// handler routine for each one.
func (g *tcpGossip) acceptLoop() {
	defer g.wg.Done()

	ctx, span := g.tracer.Start(g.options.Context, "Gossip.Listen",
		trace.WithAttributes(
			attribute.String("gossip.direction", "recv"),
			attribute.String("gossip.transport", "tcp"),
			attribute.String("gossip.bind_address", g.listener.Addr().String()),
		),
	)
	defer span.End()

	for {
		conn, err := g.listener.Accept()
		if err != nil {
			select {
			case <-g.done:
				return
			default:
				span.AddEvent("tcp.accept_error", trace.WithAttributes(attribute.String("error.message", err.Error())))
				continue
			}
		}

		g.connMtx.Lock()
		g.conns[conn] = struct{}{}
		g.connMtx.Unlock()

		g.wg.Add(1)
		go g.handleConn(ctx, conn)
	}
}

// handleConn reads a single message from conn and
// forwards it to the message channel.
func (g *tcpGossip) handleConn(ctx context.Context, conn net.Conn) {
	defer g.wg.Done()
	defer func() {
		conn.Close()
		g.connMtx.Lock()
		delete(g.conns, conn)
		g.connMtx.Unlock()
	}()

	parent := trace.SpanFromContext(ctx)
	ephemeralAddr := conn.RemoteAddr().String()

	conn.SetReadDeadline(time.Now().Add(connReadTimeout))

	// read the sender's advertised listener address.
	addrHeader := make([]byte, 4)
	if _, err := io.ReadFull(conn, addrHeader); err != nil {
		parent.AddEvent("tcp.read_error", trace.WithAttributes(
			attribute.String("error.message", err.Error()),
			attribute.String("gossip.peer_address", ephemeralAddr),
		))
		return
	}

	addrLength := binary.BigEndian.Uint32(addrHeader)
	if addrLength == 0 || addrLength > maxAddrSize {
		parent.AddEvent("tcp.read_error", trace.WithAttributes(
			attribute.String("error.message", "sender address length out of range"),
			attribute.String("gossip.peer_address", ephemeralAddr),
		))
		return
	}

	addrBuf := make([]byte, addrLength)
	if _, err := io.ReadFull(conn, addrBuf); err != nil {
		parent.AddEvent("tcp.read_error", trace.WithAttributes(
			attribute.String("error.message", err.Error()),
			attribute.String("gossip.peer_address", ephemeralAddr),
		))
		return
	}

	senderAddr := string(addrBuf)

	from, err := net.ResolveTCPAddr("tcp", senderAddr)
	if err != nil {
		parent.AddEvent("tcp.read_error", trace.WithAttributes(
			attribute.String("error.message", err.Error()),
			attribute.String("gossip.peer_address", ephemeralAddr),
		))
		return
	}

	_, child := g.tracer.Start(ctx, "Gossip.Receive", trace.WithAttributes(
		attribute.String("gossip.sender_address", senderAddr),
		attribute.String("gossip.transport", "tcp"),
	))
	defer child.End()

	// read the sender's message
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		child.RecordError(err)
		child.SetStatus(codes.Error, err.Error())
		return
	}

	length := binary.BigEndian.Uint32(header)
	if length > maxMessageSize {
		child.AddEvent("tcp.read_error", trace.WithAttributes(
			attribute.String("error.message", "message size exceeds limit"),
			attribute.Int64("gossip.message_bytes", int64(length)),
		))
		return
	}

	msg := make([]byte, length)
	if _, err := io.ReadFull(conn, msg); err != nil {
		child.RecordError(err)
		child.SetStatus(codes.Error, err.Error())
		return
	}

	child.SetAttributes(attribute.Int("gossip.message_bytes", int(length)))

	select {
	case <-g.done:
	case g.msgCh <- &gossip.Packet{From: from, Data: msg}:
	}
}
