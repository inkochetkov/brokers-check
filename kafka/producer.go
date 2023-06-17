package kafka

import (
	"context"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func (b *Broker) ProducerStart() func(ctx context.Context) error {
	return func(ctx context.Context) error {

		b.logger.Info("Producer Topics", b.Conf.Kafka.Consumer.TopickName)

		for {
			select {
			case <-ctx.Done():
				return nil
			case mes := <-b.Mes:
				err := b.sendMes(mes)
				if err != nil {
					b.logger.Error(err)
				}
			}
		}

	}
}

func (b *Broker) sendMes(msg []byte) error {

	event := make(chan kafka.Event)
	defer close(event)

	err := b.Producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &b.Conf.Kafka.Producer.TopickName, Partition: kafka.PartitionAny},
		Value:          msg,
		Key:            []byte{},
		Timestamp:      time.Time{},
		TimestampType:  0,
		Opaque:         nil,
		Headers:        []kafka.Header{{Key: b.Conf.Kafka.Producer.Key, Value: []byte(b.Conf.Kafka.Producer.Value)}},
	}, event)

	if err != nil {
		return err
	}

	e := <-event
	message := e.(*kafka.Message)

	if message.TopicPartition.Error != nil {
		return message.TopicPartition.Error
	}

	return nil

}

func (b *Broker) ProducerStop() func(ctx context.Context) error {
	return func(ctx context.Context) error {

		<-ctx.Done()
		b.Producer.Close()
		return nil

	}
}
