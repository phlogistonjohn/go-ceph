package admin

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func (suite *RadosGWTestSuite) TestUsage() {
	suite.SetupConnection()
	co, err := New(suite.endpoint, suite.accessKey, suite.secretKey, nil)
	co.HTTPClient = debugClient(co.HTTPClient)
	assert.NoError(suite.T(), err)

	suite.T().Run("get usage", func(t *testing.T) {
		usage, err := co.GetUsage(context.TODO(), Usage{ShowSummary: true})
		assert.NoError(suite.T(), err)
		assert.NotEmpty(t, usage)
	})

	suite.T().Run("trim usage", func(t *testing.T) {
		_, err := co.GetUsage(context.TODO(), Usage{RemoveAll: false})
		assert.NoError(suite.T(), err)
	})
}
