package rabbit

import (
	"time"

	"github.com/inkochetkov/log"
)

// Config Broker rabbit
type Config struct {
	Rabbit struct {
		Timeout          time.Duration `yaml:"timeout"`
		TimeoutReconnect time.Duration `yaml:"timeout_reconnect"`
		Buff             int           `yaml:"buff"`
		Producer         struct {
			Server       string `yaml:"server"`
			Queue        string `yaml:"queue"`
			Exchange     string `yaml:"exchange"`
			ExchangeType string `yaml:"exchange_type"`
			ContentType  string `yaml:"content_type"`
			Key          string `yaml:"key"`
		} `yaml:"producer"`
		Consumer struct {
			Server   string `yaml:"server"`
			Queue    string `yaml:"queue"`
			Exchange string `yaml:"exchange"`
		} `yaml:"consumer"`
	} `yaml:"rabbit"`
}

// Broker ...
type Broker struct {
	Consumer      *Conn
	MsgIn, MsgOut chan []byte
	Producer      *Conn
}

// New Broker create conn
func New(conf Config, log *log.Log) *Broker {
	broker := &Broker{}

	if conf.Rabbit.Consumer.Server != "" {
		broker.Consumer = NewConsumer(conf, log)
		broker.MsgIn = make(chan []byte)
		log.Info("Connection Rabbit Consumer")
	}
	if conf.Rabbit.Producer.Server != "" {
		broker.Producer = NewProducer(conf, log)
		broker.MsgOut = make(chan []byte)
		log.Info("Connection Rabbit Producer")
	}

	return broker
}
