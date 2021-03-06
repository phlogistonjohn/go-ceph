package cephfs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCephFSError(t *testing.T) {
	err := getError(0)
	assert.NoError(t, err)

	err = getError(-5) // IO error
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "cephfs: ret=5, Input/output error")

	err = getError(345) // no such errno
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "cephfs: ret=345")
}
