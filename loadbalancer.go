package stickypg

import (
	"context"
	"fmt"
	"math/rand/v2"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Store abstracts persistence of last-seen WAL positions (LSNs) for arbitrary keys.
// A key represents a logical entity whose reads must observe its own writes.
type Store interface {
	LSN(ctx context.Context, key string) (string, error)
	StoreLSN(ctx context.Context, key, lsn string) error
}

// Config contains DSNs for primary and standby PostgreSQL servers.
type Config struct {
	PrimaryDSN  string
	StandbyDSNs []string
}

// LoadBalancer routes read queries to standbys and writes to the primary.
// It tracks per-key LSNs to ensure "read-your-writes" consistency.
type LoadBalancer struct {
	primary  *pgxpool.Pool
	standbys []*pgxpool.Pool
	store    Store
}

// NewLoadBalancer returns a new [LoadBalancer].
// I initializes connections to primary and standby servers.
func NewLoadBalancer(ctx context.Context, servers Config, store Store) (*LoadBalancer, error) {
	primary, err := connect(ctx, servers.PrimaryDSN)
	if err != nil {
		return nil, fmt.Errorf("connect to primary: %w", err)
	}
	standbys := make([]*pgxpool.Pool, len(servers.StandbyDSNs))
	for i, url := range servers.StandbyDSNs {
		s, err := connect(ctx, url)
		if err != nil {
			return nil, fmt.Errorf("connect to standby: %w", err)
		}
		standbys[i] = s
	}

	return &LoadBalancer{
		primary:  primary,
		standbys: standbys,
		store:    store,
	}, nil
}

// Primary returns a pool handle to the primary database.
// To guarantee "read-your-writes" all writes MUST be executed through this pool.
func (lb *LoadBalancer) Primary() *pgxpool.Pool {
	return lb.primary
}

// Standby returns a standby that has replayed WAL up to at least the
// LSN associated with the given key. If no standby is sufficiently
// caught up, the primary is returned instead.
//
// This provides per-key "read-your-writes" consistency: any read
// for a key will never observe older data than a previous write for
// the same key.
func (lb *LoadBalancer) Standby(ctx context.Context, key string) (*pgxpool.Pool, error) {
	// No standbys configured—always fallback to primary.
	if len(lb.standbys) == 0 {
		return lb.primary, nil
	}

	lsn, err := lb.store.LSN(ctx, key)
	if err != nil {
		return nil, err
	}
	if lsn == "" {
		return lb.primary, nil // No stored LSN, default to the primary.
	}

	i := rand.IntN(len(lb.standbys)) //nolint:gosec
	for range len(lb.standbys) {
		sb := lb.standbys[i]
		i = (i + 1) % len(lb.standbys)

		uptodate, err := standbyLSNReached(ctx, sb, lsn)
		if err != nil {
			return nil, err // FIXME: log and continue?
		}
		if uptodate {
			return sb, nil
		}
	}
	// No caught-up standby found, force primary.
	return lb.primary, nil
}

// SaveLSN fetches the primary's current WAL LSN and stores it in the backing [Store].
// This MUST be called after a write that affects a specific key.
func (lb *LoadBalancer) SaveLSN(ctx context.Context, key string) error {
	lsn, err := getPrimaryLSN(ctx, lb.primary)
	if err != nil {
		return fmt.Errorf("retrieve LSN: %w", err)
	}
	return lb.store.StoreLSN(ctx, key, lsn)
}

// Close shuts down all database pools (primary and standbys).
func (lb *LoadBalancer) Close() {
	lb.primary.Close()
	for _, sb := range lb.standbys {
		sb.Close()
	}
}

func connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("create database pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return pool, nil
}
