package log

import (
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	log_v1 "github.com/vinicius0197/proglog/api/v1"
)

type Log struct {
	segments []*segment
	active   *segment
	dir      string
	config   *Config
	mu       sync.RWMutex
}

func newLog(dir string, config Config) (log *Log, err error) {
	// check if there are existing index files
	pattern := filepath.Join(dir, "*.index")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}

	var seg *segment

	if len(matches) == 0 { // fresh new log with no existing segments
		seg, err = newSegment(dir, config.Segment.InitialOffset, config)
		if err != nil {
			return nil, err
		}

		log = &Log{}
		log.segments = append(log.segments, seg)
		log.active = seg
		log.dir = dir
		log.config = &config

		return log, nil
	} else { // need to re-load existing segments
		existingSegments := []*segment{}
		for _, file := range matches {
			base := filepath.Base(file)
			name := strings.TrimSuffix(base, filepath.Ext(base))
			off, err := strconv.ParseUint(name, 10, 64)
			if err != nil {
				return nil, err
			}

			// re-create segments
			seg, err = newSegment(dir, off, config)
			if err != nil {
				return nil, err
			}
			existingSegments = append(existingSegments, seg)
		}

		// need to check if last segment is maxed out to set it as active or
		// create a new one
		var currentSegment *segment
		lastSegment := existingSegments[len(existingSegments)-1]
		if lastSegment.IsMaxed() {
			currentSegment, err = newSegment(dir, lastSegment.nextOffset, config)
			if err != nil {
				return nil, err
			}
		} else {
			currentSegment = lastSegment
		}

		log = &Log{}
		log.segments = append(log.segments, existingSegments...)
		log.active = currentSegment
		log.dir = dir
		log.config = &config

		return log, nil
	}
}

// Append adds a new record to the commit log
func (l *Log) Append(record *log_v1.Record) (off uint64, err error) {

	if l.active.IsMaxed() {
		newSeg, err := newSegment(l.dir, l.active.nextOffset, *l.config)
		if err != nil {
			return 0, nil
		}
		l.segments = append(l.segments, newSeg)
		l.active = newSeg

		off, err = newSeg.Append(record)
		if err != nil {
			return 0, nil
		}
		return off, nil
	} else {
		off, err = l.active.Append(record)
		if err != nil {
			return 0, err
		}
		return off, nil
	}
}

func (l *Log) Read(off uint64) (record *log_v1.Record, err error) {
	if off >= l.active.baseOffset {
		record, err = l.active.Read(off)
		if err != nil {
			return nil, err
		}

		return record, nil
	} else {
		// find the correct segment
		var matchSegment *segment
		for _, seg := range l.segments {
			if off >= seg.baseOffset {
				matchSegment = seg
			} else {
				continue
			}
		}

		record, err = matchSegment.Read(off)
		if err != nil {
			return nil, err
		}

		return record, nil
	}
}

// Close closes the active segment from the log
func (l *Log) Close() error {
	for _, seg := range l.segments {
		if err := seg.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Remove removes the log entirely
func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}

	for _, seg := range l.segments {
		if err := seg.Remove(); err != nil {
			return err
		}
	}

	return nil
}
