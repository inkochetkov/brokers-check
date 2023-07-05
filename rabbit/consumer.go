package rabbit

import (
	"context"

	"github.com/rabbitmq/amqp091-go"
)

// StartConsumer ...
func (b *Broker) StartConsumer() func(ctx context.Context) error {
	return func(ctx context.Context) error {

		msgChan, err := b.RabbitConsumer()
		if err != nil {
			b.Consumer.log.Error("RabbitConsumer", err)
			return err
		}

		for msg := range msgChan {
			select {
			case <-ctx.Done():
				return nil
			default:
				if err := msg.Ack(false); err != nil {
					continue
				}
				b.MsgIn <- msg.Body
			}
		}

		return nil
	}
}

// Consumer ...
func (b *Broker) RabbitConsumer() (<-chan amqp091.Delivery, error) {

	q, err := b.Consumer.Channel.QueueDeclare(
		b.Consumer.conf.Rabbit.Consumer.Exchange, // name
		false,                                    // durable
		false,                                    // delete when unused
		true,                                     // exclusive
		false,                                    // no-wait
		nil,                                      // arguments
	)
	if err != nil {
		return nil, err
	}

	msgChan, err := b.Consumer.Channel.Consume(
		q.Name,                                // queue
		b.Consumer.conf.Rabbit.Consumer.Queue, // consumer
		true,                                  // auto-ack
		false,                                 // exclusive
		false,                                 // no-local
		false,                                 // no-wait
		nil,                                   // args
	)
	if err != nil {
		return nil, err
	}

	return msgChan, nil

}
