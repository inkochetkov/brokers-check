package kafka

import (
	"context"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func (b *Broker) ProducerStart() func(ctx context.Context) error {
	return func(ctx context.Context) error {

		b.logger.Info("Producer Topics", b.Conf.Kafka.Consumer.TopickName)

		for mes := range b.MesOut {
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

func (b *Broker) Out(value []byte) {

	event := make(chan kafka.Event)
	defer close(event)

	err := b.Producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &b.Conf.Kafka.Producer.TopickName, Partition: kafka.PartitionAny},
		Value:          value,
		Key:            []byte{},
		Timestamp:      time.Time{},
		TimestampType:  0,
		Opaque:         nil,
		Headers:        []kafka.Header{},
	}, event)
	if err != nil {
		b.logger.Error("Send fail", err)
		return
	}

	e := <-event
	message := e.(*kafka.Message)

	if message.TopicPartition.Error != nil {
		b.logger.Error("Send fail", message.TopicPartition.Error)
	}

	b.logger.Info("message delivered", message.TopicPartition) ///
}

func (b *Broker) ProducerStop() func(ctx context.Context) error {
	return func(ctx context.Context) error {

		<-ctx.Done()
		b.Producer.Close()
		return nil

	}
}
