package redis

import (
	"context"
)

// Consumer ...
func (b *Broker) ConsumerStart() func(ctx context.Context) error {
	return func(ctx context.Context) error {

		b.logger.Info("Subscribe Topics", b.Conf.Redis.Queue)

		for {
			select {
			case <-ctx.Done():
				b.conn.Close()
				return nil
			default:
				b.Mes <- b.In()
			}
		}
	}

}

func (b *Broker) In() []byte {

	ctx, cencel := context.WithTimeout(context.Background(), b.Conf.Redis.Timeout)
	defer cencel()

	value, err := b.conn.RPop(ctx, b.Conf.Redis.Queue).Result()
	if err != nil {
		b.logger.Error(err)
		return nil
	}

	return []byte(value)
}
