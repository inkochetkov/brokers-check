package kafka

import (
	"context"
	"testing"

	"github.com/inkochetkov/config"
	"github.com/inkochetkov/gs"
	"github.com/inkochetkov/log"
	"github.com/stretchr/testify/suite"
)

var testMes = []byte("test")

// End2EndTest ...
type End2EndTest struct {
	suite.Suite
	kafka  *Broker
	module *gs.Module
	logger *log.Log
}

// TesEnd2EndTestSuite ...
func TestEnd2EndSuite(t *testing.T) {
	suite.Run(t, new(End2EndTest))
}

func (test *End2EndTest) SetupTest() {
	test.logger = log.New(log.DevLog, "", "")

	conf, err := config.NewConfig("config/config.yaml", &Config{})
	test.Require().NoError(err)
	cnfg, ok := conf.(*Config)
	if !ok {
		test.logger.Fatal("config fail", conf)
	}

	test.module = gs.New(context.Background(), test.logger)
	test.kafka = New(*cnfg, test.logger)
}

func (test *End2EndTest) TestKafka() {
	test.logger.Info("TesProducer")
	go func() {
		test.logger.Info("test mes")
		test.kafka.Mes <- testMes
		test.logger.Info("test mes out")
	}()
	test.module.Add(test.kafka.ProducerStart(), test.kafka.ProducerStop())
	test.logger.Info("TesConsumer")
	test.module.Add(test.kafka.ConsumerStart(), test.kafka.ConsumerStop())
	consumerMes := <-test.kafka.Mes
	test.Require().True(string(consumerMes) == "test")
	test.logger.Info("Test succes")

}
