package redis

import "context"

func (b *Broker) ProducerStart() func(ctx context.Context) error {
	return func(ctx context.Context) error {

		b.logger.Info("Producer Topics", b.Conf.Redis.Queue)

		for mes := range b.Mes {
			select {
			case <-ctx.Done():
				return nil
			default:
				b.Out(mes)
			}
		}
		return nil
	}
}
func (b *Broker) Out(msg []byte) {

	ctx, cencel := context.WithTimeout(context.Background(), b.Conf.Redis.Timeout)
	defer cencel()

	_, err := b.conn.LPush(ctx, b.Conf.Redis.Queue, msg).Result()
	if err != nil {
		b.logger.Error(err)

	}

}
