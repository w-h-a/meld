// Package tcp implements gossip transport over TCP with
// length-prefixed framing.
package tcp

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
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
	connReadTimeout = 30 * time.Second
)

type tcpGossip struct {
	options   gossip.Options
	listener  *net.TCPListener
	peers     []string
	msgCh     chan []byte
	done      chan struct{}
	wg        sync.WaitGroup
	listening atomic.Bool
	once      sync.Once
	mtx       sync.RWMutex
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
func (g *tcpGossip) Listen(_ context.Context) (<-chan []byte, error) {
	if !g.listening.CompareAndSwap(false, true) {
		return nil, errors.New("already listening")
	}

	g.msgCh = make(chan []byte, 64)

	g.wg.Add(1)
	go g.acceptLoop()

	return g.msgCh, nil
}

// Broadcast sends msg to all known peers via TCP. Each peer
// receives a 4-byte big-endian length header followed by the
// payload.
func (g *tcpGossip) Broadcast(ctx context.Context, msg []byte) error {
	ctx, span := g.tracer.Start(ctx, "Gossip.Broadcast",
		trace.WithAttributes(
			attribute.String("gossip.direction", "send"),
			attribute.String("gossip.transport", "tcp"),
			attribute.Int("gossip.message_bytes", len(msg)),
		),
	)
	defer span.End()

	// prepend a fixed 4-byte header containing the payload
	// length in big-endian byte order.
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(msg)))

	var lastErr error
	sent := 0

	for _, addr := range g.peers {
		if err := ctx.Err(); err != nil {
			return err
		}

		if err := g.sendTo(ctx, addr, header, msg); err != nil {
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

// Stop closes the TCP listener, waits for all connection
// handlers to exit, and closes the message channel. Safe to
// call multiple times.
func (g *tcpGossip) Stop(_ context.Context) error {
	g.once.Do(func() {
		close(g.done)
		g.listener.Close()
		g.mtx.RLock()
		for conn := range g.conns {
			conn.Close()
		}
		g.mtx.RUnlock()
		g.wg.Wait()
		if g.msgCh != nil {
			close(g.msgCh)
		}
	})
	return nil
}

func (g *tcpGossip) sendTo(ctx context.Context, addr string, header, msg []byte) error {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	if deadline, ok := ctx.Deadline(); ok {
		conn.SetWriteDeadline(deadline)
	}

	if _, err := conn.Write(header); err != nil {
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

		g.mtx.Lock()
		g.conns[conn] = struct{}{}
		g.mtx.Unlock()

		g.wg.Add(1)
		go g.handleConn(ctx, conn)
	}
}

// handleConn reads a single length-prefixed message from conn
// and forwards it to the message channel.
func (g *tcpGossip) handleConn(ctx context.Context, conn net.Conn) {
	defer g.wg.Done()
	defer func() {
		conn.Close()
		g.mtx.Lock()
		delete(g.conns, conn)
		g.mtx.Unlock()
	}()

	span := trace.SpanFromContext(ctx)
	peerAddr := conn.RemoteAddr().String()

	conn.SetReadDeadline(time.Now().Add(connReadTimeout))

	// read the 4-byte length header.
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		span.AddEvent("tcp.read_error", trace.WithAttributes(
			attribute.String("error.message", err.Error()),
			attribute.String("gossip.peer_address", peerAddr),
		))
		return
	}

	// decode the payload length from the header.
	length := binary.BigEndian.Uint32(header)

	if length > maxMessageSize {
		span.AddEvent("tcp.read_error", trace.WithAttributes(
			attribute.String("error.message", "message size exceeds limit"),
			attribute.Int64("gossip.message_bytes", int64(length)),
			attribute.String("gossip.peer_address", peerAddr),
		))
		return
	}

	// read exactly that many bytes.
	msg := make([]byte, length)
	if _, err := io.ReadFull(conn, msg); err != nil {
		span.AddEvent("tcp.read_error", trace.WithAttributes(
			attribute.String("error.message", err.Error()),
			attribute.String("gossip.peer_address", peerAddr),
		))
		return
	}

	select {
	case g.msgCh <- msg:
	case <-g.done:
	}
}
