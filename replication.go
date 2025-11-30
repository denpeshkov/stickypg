package stickypg

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

const queryTimeout = 2 * time.Second

type dbtx interface {
	QueryRow(ctx context.Context, query string, args ...any) pgx.Row
}

// standbyLSNReached checks if the standby has replayed up to or past the target LSN.
func standbyLSNReached(ctx context.Context, dbtx dbtx, lsn string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	const query = `select pg_last_wal_replay_lsn() >= $1::pg_lsn`
	var lsnReached sql.Null[bool]
	if err := dbtx.QueryRow(ctx, query, lsn).Scan(&lsnReached); err != nil {
		return false, err
	}
	if !lsnReached.Valid {
		return false, errors.New("server not in a recovery mode")
	}
	return lsnReached.V, nil
}

// getPrimaryLSN gets primay's the current WAL write location.
func getPrimaryLSN(ctx context.Context, dbtx dbtx) (string, error) {
	queryCtx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	const query = `select pg_current_wal_lsn()::text`
	var lsn string
	if err := dbtx.QueryRow(queryCtx, query).Scan(&lsn); err != nil {
		return "", err
	}
	return lsn, nil
}
