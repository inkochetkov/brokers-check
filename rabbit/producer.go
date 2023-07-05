package rabbit

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (b *Broker) StartProducer() func(ctx context.Context) error {
	return func(ctx context.Context) error {

		for msg := range b.MsgOut {
			select {
			case <-ctx.Done():
				return nil
			default:
				err := b.RabbitProducer(msg)
				if err != nil {
					b.Producer.log.Error("RabbitProducer", err)
				}
			}
		}

		return nil
	}
}

// Producer ...
func (b *Broker) RabbitProducer(msg []byte) error {

	ctx, cancel := context.WithTimeout(context.Background(), b.Producer.conf.Rabbit.Timeout)
	defer cancel()

	err := b.declareCreate()
	if err != nil {
		return err
	}

	err = b.publish(ctx, msg)
	if err != nil {
		return err
	}

	return nil
}

func (b *Broker) publish(ctx context.Context, msg []byte) error {
	err := b.Producer.Channel.PublishWithContext(ctx,
		b.Producer.conf.Rabbit.Producer.Exchange, // exchange
		b.Producer.conf.Rabbit.Producer.Key,      // routing key
		false,                                    // mandatory
		false,                                    // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  b.Producer.conf.Rabbit.Producer.ContentType,
			Body:         msg,
		})
	if err != nil {
		return err
	}

	return nil
}

func (b *Broker) declareCreate() error {

	channel := b.Producer.Channel
	logger := b.Producer.log

	var err error

	if err = channel.ExchangeDeclare(
		b.Producer.conf.Rabbit.Producer.Exchange,
		b.Producer.conf.Rabbit.Producer.ExchangeType,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		logger.Error("ExchangeDeclare", err)
		return err
	}

	if _, err := channel.QueueDeclare(
		b.Producer.conf.Rabbit.Producer.Queue,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		logger.Error("QueueDeclare", err)
		return err
	}

	if err := channel.QueueBind(
		b.Producer.conf.Rabbit.Producer.Queue,
		b.Producer.conf.Rabbit.Producer.Key,
		b.Producer.conf.Rabbit.Producer.Exchange,
		false,
		nil,
	); err != nil {
		logger.Error("QueueBind", err)
		return err
	}

	return nil
}
