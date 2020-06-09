// +build !luminous

package cephfs

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileSeekHoleData(t *testing.T) {
	mount := fsConnect(t)
	defer fsDisconnect(t, mount)
	fname := "TestFileSeekHoleData.txt"

	// set up file with data and holes
	f, err := mount.Open(fname, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	require.NoError(t, err)
	defer func() { assert.NoError(t, mount.Unlink(fname)) }()
	_, err = f.WriteAt([]byte("philipjfry"), 0)
	assert.NoError(t, err)
	_, err = f.WriteAt([]byte("turangaleela"), 64)
	assert.NoError(t, err)
	_, err = f.WriteAt([]byte("benderbrodrigez"), 128)
	assert.NoError(t, err)
	err = f.Close()
	assert.NoError(t, err)

	t.Run("seekHoles", func(t *testing.T) {
		f, err := mount.Open(fname, os.O_RDONLY, 0)
		assert.NoError(t, err)
		defer func() { assert.NoError(t, f.Close()) }()

		o, err := f.Seek(0, SeekHole)
		assert.NoError(t, err)
		assert.EqualValues(t, 11, o)

		o, err = f.Seek(64, SeekHole)
		assert.NoError(t, err)
		assert.EqualValues(t, 77, o)

		o, err = f.Seek(128, SeekHole)
		assert.NoError(t, err)
		assert.EqualValues(t, 143, o)
	})

	t.Run("seekData", func(t *testing.T) {
		f, err := mount.Open(fname, os.O_RDONLY, 0)
		assert.NoError(t, err)
		defer func() { assert.NoError(t, f.Close()) }()

		o, err := f.Seek(0, SeekData)
		assert.NoError(t, err)
		assert.EqualValues(t, 0, o)

		o, err = f.Seek(12, SeekData)
		assert.NoError(t, err)
		assert.EqualValues(t, 64, o)

		o, err = f.Seek(78, SeekData)
		assert.EqualValues(t, 128, o)
	})
}
