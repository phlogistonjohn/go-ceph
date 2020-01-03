package rados

import (
	"sort"

	"github.com/stretchr/testify/assert"
)

func writeDummyObject(suite *RadosTestSuite, ioctx *IOContext, v string) string {
	oid := suite.GenObjectName()
	err := ioctx.Write(oid, []byte(v), 0)
	assert.NoError(suite.T(), err)
	return oid
}

func (suite *RadosTestSuite) TestObjectIterator() {
	suite.SetupConnection()

	// current objs in default namespace
	prevObjectList := []string{}
	iter, err := suite.ioctx.Iter()
	assert.NoError(suite.T(), err)
	for iter.Next() {
		prevObjectList = append(prevObjectList, iter.Value())
	}
	iter.Close()
	assert.NoError(suite.T(), iter.Err())

	// create an object in a different namespace to verify that
	// iteration within a namespace does not return it
	suite.ioctx.SetNamespace("ns1")
	writeDummyObject(suite, suite.ioctx, "input data")

	// create some objects in default namespace
	suite.ioctx.SetNamespace("")
	createdList := []string{}
	for i := 0; i < 10; i++ {
		oid := writeDummyObject(suite, suite.ioctx, "input data")
		createdList = append(createdList, oid)
	}

	// prev list plus new oids
	expectedObjectList := prevObjectList
	expectedObjectList = append(expectedObjectList, createdList...)

	currObjectList := []string{}
	iter, err = suite.ioctx.Iter()
	assert.NoError(suite.T(), err)
	for iter.Next() {
		currObjectList = append(currObjectList, iter.Value())
	}
	iter.Close()
	assert.NoError(suite.T(), iter.Err())

	// curr list doesn't include the obj in ns1
	sort.Strings(expectedObjectList)
	sort.Strings(currObjectList)
	assert.Equal(suite.T(), currObjectList, expectedObjectList)
}

func (suite *RadosTestSuite) TestObjectIteratorAcrossNamespaces() {
	suite.SetupConnection()

	const perNamespace = 100

	// tests use a shared pool so namespaces need to be unique across tests.
	// below ns1=nsX and ns2=nsY. ns1 is used elsewhere.
	objectListNS1 := []string{}
	objectListNS2 := []string{}

	// populate list of current objects
	suite.ioctx.SetNamespace(RadosAllNamespaces)
	existingList := []string{}
	iter, err := suite.ioctx.Iter()
	assert.NoError(suite.T(), err)
	for iter.Next() {
		existingList = append(existingList, iter.Value())
	}
	iter.Close()
	assert.NoError(suite.T(), iter.Err())

	// create some new objects in namespace: nsX
	createdList := []string{}
	suite.ioctx.SetNamespace("nsX")
	for i := 0; i < 10; i++ {
		oid := writeDummyObject(suite, suite.ioctx, "input data")
		createdList = append(createdList, oid)
	}
	assert.True(suite.T(), len(createdList) == 10)

	// create some new objects in namespace: nsY
	suite.ioctx.SetNamespace("nsY")
	for i := 0; i < 10; i++ {
		oid := writeDummyObject(suite, suite.ioctx, "input data")
		createdList = append(createdList, oid)
	}
	assert.True(suite.T(), len(createdList) == 20)

	suite.ioctx.SetNamespace(RadosAllNamespaces)
	iter, err = suite.ioctx.Iter()
	assert.NoError(suite.T(), err)
	rogueList := []string{}
	for iter.Next() {
		if iter.Namespace() == "nsX" {
			objectListNS1 = append(objectListNS1, iter.Value())
		} else if iter.Namespace() == "nsY" {
			objectListNS2 = append(objectListNS2, iter.Value())
		} else {
			rogueList = append(rogueList, iter.Value())
		}
	}
	iter.Close()
	assert.NoError(suite.T(), iter.Err())

	assert.Equal(suite.T(), len(existingList), len(rogueList))
	assert.Equal(suite.T(), len(objectListNS1), 10)
	assert.Equal(suite.T(), len(objectListNS2), 10)

	objectList := []string{}
	objectList = append(objectList, objectListNS1...)
	objectList = append(objectList, objectListNS2...)
	sort.Strings(objectList)
	sort.Strings(createdList)

	assert.Equal(suite.T(), objectList, createdList)

	sort.Strings(rogueList)
	sort.Strings(existingList)
	assert.Equal(suite.T(), rogueList, existingList)
}
