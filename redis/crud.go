package redis

import (
	"context"

	"github.com/go-redis/redis/v8"
)

// Set ...
func (r *Broker) Set(ctx context.Context, key string, value any) error {

	err := r.conn.Set(ctx, r.Conf.Redis.Key, value, 0).Err()
	if err != nil {
		r.logger.Error("Set", err)
		return err
	}

	return nil
}

// Get ...
func (r *Broker) Get(ctx context.Context, key string) (string, error) {

	value, err := r.conn.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return "", nil
		}
		return "", err
	}

	return value, nil
}

// Del ...
func (r *Broker) Del(ctx context.Context, key string) error {

	_, err := r.conn.Del(ctx, key).Result()
	if err != nil {
		return err
	}

	return nil
}
