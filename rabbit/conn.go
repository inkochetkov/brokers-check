package rabbit

import (
	"fmt"
	"sync"
	"time"

	"github.com/inkochetkov/log"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	ProducerName         = "Producer"
	ConsumerNmae         = "Consumer"
	ReconnectChannelName = "Channel reconnect"
)

// Conn - connection rabbit
type Conn struct {
	mu         sync.RWMutex
	Channel    *amqp.Channel
	connection *amqp.Connection
	conf       Config
	log        *log.Log
}

// NewProducer - start producer connection
func NewProducer(conf Config, log *log.Log) *Conn {
	conn := &Conn{conf: conf, log: log}
	err := conn.firstConnect(conf.Rabbit.Producer.Server)
	if err != nil {
		log.Error(ProducerName, err)
	}
	return conn
}

// NewConsumer - start consumer connection
func NewConsumer(conf Config, log *log.Log) *Conn {
	conn := &Conn{conf: conf, log: log}
	err := conn.firstConnect(conf.Rabbit.Consumer.Server)
	if err != nil {
		log.Error(ConsumerNmae, err)
	}
	return conn
}

// firstConnect
func (c *Conn) firstConnect(url string) (err error) {

	if c.connection == nil {

		c.connection, err = c.connect(url)
		if err != nil {
			return fmt.Errorf("connection is not open, %w", err)
		}

		err = c.channel()
		if err != nil {
			return fmt.Errorf("channel is not open, %w", err)
		}

		go c.reconnect(url)
	}

	return
}

// get channel
func (c *Conn) channel() (err error) {
	c.Channel, err = c.connection.Channel()
	return
}

// get connect
func (c *Conn) connect(url string) (*amqp.Connection, error) {

	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

// gorutine reconnect
func (c *Conn) reconnect(url string) {

WATCH:
	conErr := <-c.connection.NotifyClose(make(chan *amqp.Error))
	if conErr != nil {
		var err error
		for {
			timer := time.NewTimer(c.conf.Rabbit.TimeoutReconnect)
			<-timer.C
			c.mu.RLock()
			c.connection, err = amqp.Dial(url)
			c.mu.RUnlock()
			if err == nil {
				err := c.channel()
				if err != nil {
					c.log.Error(ReconnectChannelName, err)
				}
				goto WATCH

			}
		}
	}
}
