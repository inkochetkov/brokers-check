package redis

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
	module *gs.Module
	logger *log.Log
	redis  *Broker
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
	test.redis = New(*cnfg, test.logger)
}

func (test *End2EndTest) TestRedis() {

	test.logger.Info("TesProducer")
	test.redis.Out(testMes)

	test.logger.Info("TesConsumer")
	mes := test.redis.In()
	test.Require().True(string(mes) == "test")

	test.logger.Info("Test succes")
}
