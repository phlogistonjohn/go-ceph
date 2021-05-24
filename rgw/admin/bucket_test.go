package admin

import (
	"context"
	"log"
	"net/http"
	"net/http/httputil"
	"testing"

	"github.com/stretchr/testify/assert"
)

type traceClient struct {
	HTTPClient HTTPClient
}

func (t *traceClient) Do(request *http.Request) (*http.Response, error) {
	dump, err := httputil.DumpRequestOut(request, true)
	if err != nil {
		return nil, err
	}
	log.Printf("\n--- request ---\n%s\n", string(dump))

	// Send HTTP request
	resp, err := t.HTTPClient.Do(request)
	if err != nil {
		log.Printf("\n--- error ---\n%s\n", err)
		return nil, err
	}

	dump, err = httputil.DumpResponse(resp, true)
	if err != nil {
		return nil, err
	}
	log.Printf("\n--- response ---\n%s\n", string(dump))

	return resp, nil
}

func debugClient(c HTTPClient) HTTPClient {
	return &traceClient{c}
}

func (suite *RadosGWTestSuite) TestBucket() {
	suite.SetupConnection()
	co, err := New(suite.endpoint, suite.accessKey, suite.secretKey, nil)
	co.HTTPClient = debugClient(co.HTTPClient)
	assert.NoError(suite.T(), err)

	suite.T().Run("list buckets", func(t *testing.T) {
		buckets, err := co.ListBuckets(context.TODO())
		assert.NoError(suite.T(), err)
		assert.Equal(suite.T(), 0, len(buckets))
	})

	suite.T().Run("info non-existing bucket", func(t *testing.T) {
		_, err := co.GetBucketInfo(context.TODO(), "foo")
		assert.Error(suite.T(), err)
		// TODO: report to rgw team, this should return NoSuchBucket
		assert.True(suite.T(), IsNoSuchKey(err))
	})

	suite.T().Run("remove non-existing bucket", func(t *testing.T) {
		err := co.RemoveBucket(context.TODO(), "foo")
		assert.Error(suite.T(), err)
		// TODO: report to rgw team, this should return NoSuchBucket?
		assert.True(suite.T(), IsNoSuchKey(err))
	})
}
