package kafka

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// ConsumerStart ...
func (b *Broker) ConsumerStart() func(ctx context.Context) error {
	return func(ctx context.Context) error {

		b.Subscriber()

		for {
			select {
			case <-ctx.Done():
				return nil
			default:
				b.MesIn <- b.In()
			}
		}
	}
}

// Subscriber topic
func (b *Broker) Subscriber() {

	b.logger.Info("Subscribe Topics", b.Conf.Kafka.Consumer.TopickName)

	err := b.Consumer.Subscribe(b.Conf.Kafka.Consumer.TopickName, nil)
	if err != nil {
		b.logger.Fatal(err)
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

// Read topic
func (b *Broker) In() []byte {

	event := b.Consumer.Poll(b.Conf.Kafka.TimeoutMS)
	if event == nil {
		b.logger.Error("event  nil") ///
		return nil
	}

	switch msg := event.(type) {
	case kafka.Error:
		b.logger.Error(msg.Error())
		return nil
	case *kafka.Message:
		return msg.Value
	}

	return nil
}
