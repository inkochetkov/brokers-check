package kafka

import (
	"context"
)

// ConsumerStart ...
func (b *Broker) ConsumerStart() func(ctx context.Context) error {
	return func(ctx context.Context) error {

		err := b.Consumer.SubscribeTopics([]string{b.Conf.Kafka.Consumer.TopickName}, nil)
		if err != nil {
			b.logger.Error(err)
			return err
		}

		b.logger.Info("Subscribe Topics", b.Conf.Kafka.Consumer.TopickName)

		for {
			select {
			case <-ctx.Done():
				b.Consumer.Close()
				return nil
			default:
				msg, err := b.Consumer.ReadMessage(b.Conf.Kafka.Timeout)
				if err != nil {
					b.logger.Error(err)
					continue
				}
				b.Mes <- msg.Value
			}
		}
	}
}

// ConsumerStop ...
func (b *Broker) ConsumerStop() func(ctx context.Context) error {
	return func(ctx context.Context) error {
		<-ctx.Done()
		b.Consumer.Close()
		return nil
	}
}
