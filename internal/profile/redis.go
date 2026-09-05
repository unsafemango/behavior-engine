package profile

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisCache holds the fast-lookup copy of each profile. It is
// deliberately dumb: JSON blob in, JSON blob out, with a TTL so a user
// who goes quiet eventually falls out of cache instead of living there
// forever. The strategy API treats a miss as "go read Postgres", not as
// an error.
type RedisCache struct {
	client *redis.Client
	ttl    time.Duration
}

func NewRedisCache(client *redis.Client, ttl time.Duration) *RedisCache {
	return &RedisCache{client: client, ttl: ttl}
}

func key(userID string) string {
	return "profile:" + userID
}

// Set writes/refreshes the cached profile.
func (c *RedisCache) Set(ctx context.Context, p Profile) error {
	b, err := json.Marshal(p)
	if err != nil {
		return err
	}
	return c.client.Set(ctx, key(p.UserID), b, c.ttl).Err()
}

// Get returns (profile, true, nil) on a hit, (zero, false, nil) on a
// clean miss, and (zero, false, err) only on an actual Redis error.
func (c *RedisCache) Get(ctx context.Context, userID string) (Profile, bool, error) {
	b, err := c.client.Get(ctx, key(userID)).Bytes()
	if errors.Is(err, redis.Nil) {
		return Profile{}, false, nil
	}
	if err != nil {
		return Profile{}, false, err
	}
	var p Profile
	if err := json.Unmarshal(b, &p); err != nil {
		return Profile{}, false, err
	}
	return p, true, nil
}

// Ping is used by the readiness probe.
func (c *RedisCache) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return c.client.Ping(ctx).Err()
}
