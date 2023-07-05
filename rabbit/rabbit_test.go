package rabbit

import (
	"testing"

	"github.com/inkochetkov/config"
	"github.com/inkochetkov/log"
	"github.com/stretchr/testify/suite"
)

var testMes = []byte("test-rabbit")

type End2EndTest struct {
	suite.Suite
	rabbit *Broker
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
	test.rabbit = New(*cnfg, test.logger)
}

func (test *End2EndTest) TestRabbit() {
	test.logger.Info("Producer")
	err := test.rabbit.RabbitProducer(testMes)
	test.Require().NoError(err)

	test.logger.Info("TesConsumer")
	chanDelivery, err := test.rabbit.RabbitConsumer()
	test.Require().NoError(err)
	mes := <-chanDelivery
	test.Require().True(string(mes.Body) == "test-kafka")

	test.logger.Info("Test succes")
}
