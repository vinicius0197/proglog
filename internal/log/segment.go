package log

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type segment struct {
	store      *store
	index      *index
	baseOffset uint64
	nextOffset uint64
	config     Config
}

func newSegment(dir string, baseOffset uint64, config Config) (*segment, error) {
	fs, err := os.OpenFile(
		filepath.Join(dir, fmt.Sprintf("%020d.store", baseOffset)),
		os.O_CREATE|os.O_APPEND|os.O_RDWR,
		0644,
	)
	if err != nil {
		return nil, err
	}
	store, err := newStore(fs)
	if err != nil {
		return nil, err
	}

	fi, err := os.OpenFile(
		filepath.Join(dir, fmt.Sprintf("%020d.index", baseOffset)),
		os.O_CREATE|os.O_APPEND|os.O_RDWR,
		0644,
	)
	if err != nil {
		return nil, err
	}

	index, err := newIndex(fi, config)
	if err != nil {
		return nil, err
	}

	// determine nextOffset
	out, _, err := index.Read(-1)

	nextOffset := baseOffset + uint64(out) + 1
	if err == io.EOF {
		nextOffset = baseOffset
	}

	return &segment{
		store:      store,
		index:      index,
		baseOffset: baseOffset,
		nextOffset: nextOffset,
		config:     config,
	}, nil
}
