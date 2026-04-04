package log

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	log_v1 "github.com/vinicius0197/proglog/api/v1"
)

func TestLog(t *testing.T) {
	dir := t.TempDir()
	config := Config{}
	config.Segment.MaxStoreBytes = 1024
	config.Segment.MaxIndexBytes = entWidth * 3
	config.Segment.InitialOffset = 0

	setup := func(t *testing.T) *Log {
		t.Helper()

		log, err := newLog(dir, config)
		require.NoError(t, err)

		t.Cleanup(func() { log.Remove() })
		return log
	}

	t.Run("creates a fresh Log pointing to active segment", func(t *testing.T) {
		log := setup(t)

		_, err := os.Stat(log.active.store.Name()) // checks that a store file has been created for the active segment
		require.NoError(t, err)
	})

	t.Run("restarts safely without data loss when re-creating log", func(t *testing.T) {
		log, err := newLog(dir, config)
		require.NoError(t, err)

		record := &log_v1.Record{}
		value := []byte("record")

		record.Value = value

		for i := 0; i < 5; i++ {
			log.Append(record)
		}

		err = log.Close()
		require.NoError(t, err)

		log, err = newLog(dir, config)
		require.Equal(t, 2, len(log.segments))

		// cleanup
		log.Remove()
	})

	t.Run("can read from the correct segment given an offset", func(t *testing.T) {
		log := setup(t)

		record := &log_v1.Record{}

		offsets := []uint64{}

		for i := 0; i < 5; i++ {
			value := []byte(fmt.Sprintf("record_%d", i))
			record.Value = value
			off, _ := log.Append(record)
			offsets = append(offsets, off)
		}

		// read from the given offset
		record, _ = log.Read(offsets[0])
		require.Equal(t, "record_0", string(record.Value))
	})
}
