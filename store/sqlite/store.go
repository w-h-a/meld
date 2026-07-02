package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/w-h-a/meld/store"
	"go.nhat.io/otelsql"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"

	_ "modernc.org/sqlite"
)

var driverName string

func init() {
	name, err := otelsql.Register(
		"sqlite",
		otelsql.TraceQueryWithoutArgs(),
		otelsql.TraceRowsClose(),
		otelsql.TraceRowsAffected(),
		otelsql.WithSystem(semconv.DBSystemSqlite),
	)
	if err != nil {
		detail := fmt.Errorf("register otel sqlite driver: %w", err)
		panic(detail)
	}

	driverName = name
}

type sqliteStore struct {
	options store.Options
	conn    *sql.DB
	tracer  trace.Tracer
}

func New(opts ...store.Option) (store.Store, error) {
	options := store.NewOptions(opts...)

	conn, err := sql.Open(driverName, options.Location)
	if err != nil {
		return nil, err
	}

	conn.SetMaxOpenConns(1)

	if _, err := conn.ExecContext(context.Background(), schema); err != nil {
		conn.Close()
		return nil, err
	}

	if err := otelsql.RecordStats(conn); err != nil {
		conn.Close()
		return nil, err
	}

	s := &sqliteStore{
		options: options,
		conn:    conn,
		tracer:  otel.Tracer("meld/store/sqlite"),
	}

	return s, nil
}

// Save persists data as the node's single durable slot, replacing any
// prior blob in one atomic, idempotent upsert.
func (s *sqliteStore) Save(ctx context.Context, data []byte) error {
	ctx, span := s.tracer.Start(ctx, "Store.Save", trace.WithAttributes(
		attribute.String("store.location", s.options.Location),
		attribute.Int("store.bytes_written", len(data)),
	))
	defer span.End()

	if data == nil {
		data = []byte{}
	}

	_, err := s.conn.ExecContext(
		ctx,
		`INSERT INTO state (id, data) VALUES(1, ?) ON CONFLICT(id) DO UPDATE SET data = excluded.data`,
		data,
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	return nil
}

// Load returns the node's most recently saved blob, reporting false
// with no error when nothing has been written yet, so that a fresh
// node distinguishes no-prior-state from a failure.
func (s *sqliteStore) Load(ctx context.Context) ([]byte, bool, error) {
	ctx, span := s.tracer.Start(ctx, "Store.Load", trace.WithAttributes(
		attribute.String("store.location", s.options.Location),
	))
	defer span.End()

	var data []byte

	err := s.conn.QueryRowContext(ctx, `SELECT data FROM state WHERE id = 1`).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		span.SetAttributes(attribute.Bool("store.found", false))
		return nil, false, nil
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, false, err
	}

	span.SetAttributes(attribute.Bool("store.found", true))

	return data, true, nil
}

// Close releases the sqlite connection pool.
func (s *sqliteStore) Close(ctx context.Context) error {
	return s.conn.Close()
}
