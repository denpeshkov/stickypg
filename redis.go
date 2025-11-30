package stickypg

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisStore implements [Store] backed by Redis.
type RedisStore struct {
	rdb    *redis.Client
	expiry time.Duration
}

// NewRedisStore constructs a [RedisStore] with TTL-based eviction for keys.
func NewRedisStore(rdb *redis.Client, expiry time.Duration) *RedisStore {
	return &RedisStore{
		rdb:    rdb,
		expiry: expiry,
	}
}

// StoreLSN saves the given LSN for a specific key.
func (s *RedisStore) StoreLSN(ctx context.Context, key string, lsn string) error {
	return s.rdb.Set(ctx, s.key(key), lsn, s.expiry).Err()
}

// LSN returns the LSN stored for the ket, or an empty string if not found.
func (s *RedisStore) LSN(ctx context.Context, key string) (string, error) {
	lsn, err := s.rdb.Get(ctx, s.key(key)).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil // No stored location.
	}
	return lsn, err
}

// DeleteLSN removes the stored LSN for a key.
func (s *RedisStore) DeleteLSN(ctx context.Context, key string) error {
	return s.rdb.Del(ctx, s.key(key)).Err()
}

func (s *RedisStore) key(key string) string {
	return "stickypg:lsn:key:" + key
}
