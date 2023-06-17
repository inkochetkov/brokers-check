package kafka

import (
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/inkochetkov/log"
)

// Config Broker Kafka
type Config struct {
	Kafka struct {
		Timeout  time.Duration `yaml:"timeout"`
		Producer struct {
			Server     string `yaml:"server"`
			TopickName string `yaml:"topic"`
			Key        string `yaml:"key"`
			Value      string `yaml:"value"`
		} `yaml:"producer"`
		Consumer struct {
			Server          string `yaml:"server"`
			GroupID         string `yaml:"group_id"`
			AutoOffsetReset string `yaml:"auto_offset_reset"`
			TopickName      string `yaml:"topic"`
		} `yaml:"consumer"`
	} `yaml:"kafka"`
}

// Broker Kafka
type Broker struct {
	Producer *kafka.Producer
	Consumer *kafka.Consumer

	Conf   Config
	Mes    chan []byte
	logger *log.Log
}

// New Broker Kafka
func New(conf Config, log *log.Log) *Broker {
	broker := &Broker{
		Mes:    make(chan []byte),
		logger: log,
		Conf:   conf}

	var err error
	if conf.Kafka.Producer.Server != "" {
		broker.Producer, err = kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers": conf.Kafka.Producer.Server,
		})
		if err != nil {
			log.Fatal(err)
		}
		log.Info("Connection Kafka Producer")
	}
	if conf.Kafka.Consumer.Server != "" {
		broker.Consumer, err = kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers": conf.Kafka.Consumer.Server,
			"group.id":          conf.Kafka.Consumer.GroupID,
			"auto.offset.reset": conf.Kafka.Consumer.AutoOffsetReset,
		})
		if err != nil {
			return nil
		}
		log.Info("Connection Kafka Consumer")
	}

	return broker
}
