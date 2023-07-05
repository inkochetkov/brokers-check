package redis

import (
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/inkochetkov/log"
)

// Config Broker Redis
type Config struct {
	Redis struct {
		Timeout time.Duration `yaml:"timeout"`
		Server  string        `yaml:"server"`
		Queue   string        `yaml:"queue"`
		Buff    int           `yaml:"buff"`
		Key     string        `yaml:"key"`
	} `yaml:"redis"`
}

// Broker Kafka
type Broker struct {
	conn *redis.Client

	Conf   Config
	Mes    chan []byte
	logger *log.Log
}

// New create conn
func New(conf Config, log *log.Log) *Broker {
	broker := &Broker{
		Mes:    make(chan []byte, conf.Redis.Buff),
		logger: log,
		Conf:   conf}

	option, err := redis.ParseURL(conf.Redis.Server)
	if err != nil {
		log.Fatal(err)
	}

	broker.conn = redis.NewClient(option)

	return broker
}
