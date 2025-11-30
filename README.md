# PostgreSQL Load Balancer with Read-After-Write Consistency

A Go implementation that routes writes to the primary and reads to standbys only when they are up to date,
ensuring per-key read-after-write consistency across replicas

## Usage

```go
func main() {
	ctx := context.Background()
	userID := "user_42" // In this example we use user ID as a key.

	store := NewRedisStore(
		redis.NewClient(&redis.Options{Addr: "localhost:6379"}),
		1*time.Minute,
	)

	lb, err := NewLoadBalancer(
		ctx,
		Config{
			PrimaryDSN: "postgres://user:pass@primary:5432/dbname",
			StandbyDSNs: []string{
				"postgres://user:pass@replica1:5432/dbname",
				"postgres://user:pass@replica2:5432/dbname"},
		},
		store,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer lb.Close()

	// Read-only query executed on an up-to-date standby, or falls back to primary.
	{
		// Returns a standby that has replayed up to the required LSN for the given user;
		// otherwise returns the primary.
		conn, err := lb.Standby(ctx, userID)
		if err != nil {
			log.Fatal(err)
		}

		var name string
		if err := conn.QueryRow(ctx, "select name from users where id = $1", userID).Scan(&name); err != nil {
			log.Fatal(err)
		}
	}

	// Read-write query executed on primary.
	{
		conn := lb.Primary()
		if _, err = conn.Exec(ctx, "update users set name = $1 where id = $2", "John", userID); err != nil {
			log.Fatal(err)
		}

		// Persist LSN for the given user in Redis.
		lb.SaveLSN(ctx, userID)
	}
}
```
