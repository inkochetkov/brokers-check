package kafka

import (
	"testing"

	"github.com/inkochetkov/config"
	"github.com/inkochetkov/log"
	"github.com/stretchr/testify/suite"
)

var testMes = []byte("test-kafka")

// End2EndTest ...
type End2EndTest struct {
	suite.Suite
	kafka  *Broker
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
	test.logger.Info("config", cnfg)
	test.kafka = New(*cnfg, test.logger)
}

func (test *End2EndTest) TestKafka() {

	test.logger.Info("Producer")
	test.kafka.Out(testMes)
	test.kafka.Producer.Close()

	test.logger.Info("TesConsumer")
	test.kafka.Subscriber()
	mes := test.kafka.In()
	test.Require().True(string(mes) == "test-kafka")
	test.kafka.Consumer.Close()

	test.logger.Info("Test succes")
}
