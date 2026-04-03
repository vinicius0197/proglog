package log

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	log_v1 "github.com/vinicius0197/proglog/api/v1"
	"google.golang.org/protobuf/proto"
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

// Append coordinates storing data between the store and index
func (s *segment) Append(record *log_v1.Record) (off uint64, err error) {
	record.Offset = s.nextOffset
	serializedRecord, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	_, pos, err := s.store.Append(serializedRecord)
	if err != nil {
		return 0, err
	}

	if err := s.index.Write(uint32(s.nextOffset)-uint32(s.baseOffset), pos); err != nil {
		return 0, err
	}

	off = s.nextOffset
	s.nextOffset++

	return off, nil
}

// Read receives an absolute offset and returns a protobuf encoded record
func (s *segment) Read(off uint64) (record *log_v1.Record, err error) {
	relativeOffset := off - s.baseOffset
	record = &log_v1.Record{}

	_, pos, err := s.index.Read(int64(relativeOffset))
	if err != nil {
		return nil, err
	}

	// get the record
	r, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}

	if err := proto.Unmarshal(r, record); err != nil {
		return nil, err
	}

	return record, nil
}

// IsMaxed returns true if the segment index or store is maxed out
func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes
}

// Close closes first the index and then the store
func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}

	if err := s.store.Close(); err != nil {
		return err
	}

	return nil
}

func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}

	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}

	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}

	return nil
}
