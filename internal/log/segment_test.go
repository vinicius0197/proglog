package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	log_v1 "github.com/vinicius0197/proglog/api/v1"
)

func TestSegment(t *testing.T) {
	config := Config{}
	config.Segment.MaxStoreBytes = 1024
	config.Segment.MaxIndexBytes = entWidth * 3

	dir := t.TempDir()

	setup := func(t *testing.T) *segment {
		t.Helper()

		seg, err := newSegment(dir, 0, config)
		require.NoError(t, err)
		t.Cleanup(func() { seg.Remove() }) // runs after the subtest
		return seg
	}

	t.Run("creates index file", func(t *testing.T) {
		segment := setup(t)
		_, err := os.Stat(segment.index.Name())
		require.NoError(t, err)
	})

	t.Run("creates store file", func(t *testing.T) {
		segment := setup(t)
		_, err := os.Stat(segment.store.Name())
		require.NoError(t, err)
	})

	t.Run("nextOffset should be equal to base offset", func(t *testing.T) {
		segment := setup(t)
		require.Equal(t, segment.nextOffset, uint64(0))
	})

	t.Run("can read appended record on segment", func(t *testing.T) {
		segment := setup(t)
		value := []byte("hello world")
		record := &log_v1.Record{}
		record.Value = value
		off, err := segment.Append(record)

		require.NoError(t, err)

		r, err := segment.Read(off)

		require.NoError(t, err)
		require.Equal(t, value, r.Value)
	})

	t.Run("marks segment as maxed after enough writes", func(t *testing.T) {
		segment := setup(t)

		value := []byte("record")
		record := &log_v1.Record{}
		record.Value = value

		// append 3 times
		for i := 0; i < 3; i++ {
			_, err := segment.Append(record)
			require.NoError(t, err)
		}

		require.True(t, segment.IsMaxed())
	})

	t.Run("has the correct offset after segment restart", func(t *testing.T) {
		segment := setup(t)

		value := []byte("recovery_record")
		record := &log_v1.Record{}
		record.Value = value

		_, err := segment.Append(record)
		require.NoError(t, err)
		currentNextOffset := segment.nextOffset

		err = segment.Close()
		require.NoError(t, err)

		seg, err := newSegment(dir, 0, config)

		require.Equal(t, currentNextOffset, seg.nextOffset)
	})
}
