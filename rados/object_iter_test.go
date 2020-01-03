package rados

import (
	"fmt"
	"sort"

	"github.com/stretchr/testify/assert"
)

func writeDummyObject(suite *RadosTestSuite, ioctx *IOContext, v string) string {
	oid := suite.GenObjectName()
	err := ioctx.Write(oid, []byte(v), 0)
	assert.NoError(suite.T(), err)
	return oid
}

func cleanObjects(suite *RadosTestSuite, ns string, toDelete []string) {
	suite.ioctx.SetNamespace(ns)
	for _, oid := range toDelete {
		err := suite.ioctx.Delete(oid)
		assert.NoError(suite.T(), err)
	}
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
	defer cleanObjects(suite, "", createdList)

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
	nsXObjects := []string{}
	suite.ioctx.SetNamespace("nsX")
	for i := 0; i < 10; i++ {
		oid := writeDummyObject(suite, suite.ioctx, "input data")
		nsXObjects = append(nsXObjects, oid)
	}
	defer cleanObjects(suite, "nsX", nsXObjects)
	assert.Equal(suite.T(), 10, len(nsXObjects))

	// create some new objects in namespace: nsY
	nsYObjects := []string{}
	suite.ioctx.SetNamespace("nsY")
	for i := 0; i < 10; i++ {
		oid := writeDummyObject(suite, suite.ioctx, "input data")
		nsYObjects = append(nsYObjects, oid)
	}
	defer cleanObjects(suite, "nsY", nsYObjects)
	assert.Equal(suite.T(), 10, len(nsYObjects))

	createdList := []string{}
	createdList = append(createdList, nsXObjects...)
	createdList = append(createdList, nsYObjects...)

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

func (suite *RadosTestSuite) TestObjectsIterList() {
	suite.SetupConnection()

	initialObjectNames := []string{}
	suite.ioctx.SetNamespace(RadosAllNamespaces)
	oiter, err := NewObjectsIter(suite.ioctx)
	assert.NoError(suite.T(), err)

	for {
		entry, err := oiter.Next()
		if err == RadosErrorNotFound {
			break
		}
		if assert.NoError(suite.T(), err) {
			initialObjectNames = append(initialObjectNames, entry.Entry)
		}
	}
	oiter.Close()

	suite.ioctx.SetNamespace("test-ns-1")
	created := []string{}
	for i := 0; i < 10; i++ {
		oid := writeDummyObject(
			suite, suite.ioctx, fmt.Sprintf("input data %d", i))
		created = append(created, oid)
	}
	defer cleanObjects(suite, "test-ns-1", created)

	ns1ObjectNames := []string{}
	oiter, err = NewObjectsIter(suite.ioctx)
	assert.NoError(suite.T(), err)
	for {
		entry, err := oiter.Next()
		if err == RadosErrorNotFound {
			break
		}
		if assert.NoError(suite.T(), err) {
			ns1ObjectNames = append(ns1ObjectNames, entry.Entry)
		}
	}
	oiter.Close()

	sort.Strings(created)
	sort.Strings(ns1ObjectNames)
	assert.Equal(suite.T(), created, ns1ObjectNames)

	expected := []string{}
	expected = append(expected, initialObjectNames...)
	expected = append(expected, ns1ObjectNames...)

	suite.ioctx.SetNamespace("test-ns-2")
	oid := writeDummyObject(suite, suite.ioctx, "more data")
	defer cleanObjects(suite, "test-ns-2", []string{oid})
	expected = append(expected, oid)

	currObjectNames := []string{}
	foundNs := map[string]int{}
	suite.ioctx.SetNamespace(RadosAllNamespaces)
	oiter, err = NewObjectsIter(suite.ioctx)
	assert.NoError(suite.T(), err)
	for {
		entry, err := oiter.Next()
		if err == RadosErrorNotFound {
			break
		}
		if assert.NoError(suite.T(), err) {
			currObjectNames = append(currObjectNames, entry.Entry)
			foundNs[entry.Namespace] += 1
		}
	}
	oiter.Close()

	sort.Strings(expected)
	sort.Strings(currObjectNames)
	assert.Equal(suite.T(), expected, currObjectNames)
	assert.Equal(suite.T(), 10, foundNs["test-ns-1"])
	assert.Equal(suite.T(), 1, foundNs["test-ns-2"])
}

func (suite *RadosTestSuite) TestObjectsIterSeekToken() {
	suite.SetupConnection()

	suite.ioctx.SetNamespace("test-ns-1")
	created := []string{}
	for i := 0; i < 10; i++ {
		oid := writeDummyObject(
			suite, suite.ioctx, fmt.Sprintf("input data %d", i))
		created = append(created, oid)
	}
	defer cleanObjects(suite, "test-ns-1", created)

	oiter, err := NewObjectsIter(suite.ioctx)
	assert.NoError(suite.T(), err)

	token1 := oiter.Token()
	names1 := []string{}
	for {
		entry, err := oiter.Next()
		if err == RadosErrorNotFound {
			break
		}
		if assert.NoError(suite.T(), err) {
			names1 = append(names1, entry.Entry)
		}
	}

	oiter.Seek(token1)
	names2 := []string{}
	for {
		entry, err := oiter.Next()
		if err == RadosErrorNotFound {
			break
		}
		if assert.NoError(suite.T(), err) {
			names2 = append(names2, entry.Entry)
		}
	}
	oiter.Close()

	assert.Equal(suite.T(), 10, len(names1))
	assert.Equal(suite.T(), 10, len(names2))
	assert.Equal(suite.T(), names1, names2)
}
