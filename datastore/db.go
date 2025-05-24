package datastore

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const (
	outFileName    = "current-data"
	segmentPrefix  = "segment-"
	defaultMaxSize = 10 * 1024 * 1024 // 10MB
)

var ErrNotFound = fmt.Errorf("record does not exist")

type hashIndex map[string]int64

type Db struct {
	out       *os.File
	outOffset int64
	outPath   string
	dir       string
	index     hashIndex
	segments  []*Segment
	maxSize   int64
	mu        sync.Mutex
}

type Segment struct {
	path  string
	index hashIndex
}

func Open(dir string) (*Db, error) {
	return OpenWithMaxSize(dir, defaultMaxSize)
}

func OpenWithMaxSize(dir string, maxSize int64) (*Db, error) {
	outputPath := filepath.Join(dir, outFileName)
	f, err := os.OpenFile(outputPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}

	db := &Db{
		out:     f,
		outPath: outputPath,
		dir:     dir,
		index:   make(hashIndex),
		maxSize: maxSize,
	}

	err = db.recover()
	if err != nil && err != io.EOF {
		return nil, err
	}

	err = db.loadSegments()
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (db *Db) loadSegments() error {
	files, err := os.ReadDir(db.dir)
	if err != nil {
		return err
	}

	var segmentFiles []string
	for _, file := range files {
		name := file.Name()
		if name != outFileName && len(name) > len(segmentPrefix) && name[:len(segmentPrefix)] == segmentPrefix {
			segmentFiles = append(segmentFiles, name)
		}
	}

	sort.Strings(segmentFiles)

	for _, segFile := range segmentFiles {
		segPath := filepath.Join(db.dir, segFile)
		seg, err := db.loadSegment(segPath)
		if err != nil {
			return err
		}
		db.segments = append(db.segments, seg)
	}

	return nil
}

func (db *Db) loadSegment(path string) (*Segment, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	seg := &Segment{
		path:  path,
		index: make(hashIndex),
	}

	in := bufio.NewReader(file)
	var offset int64
	for {
		var record entry
		n, err := record.DecodeFromReader(in)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		seg.index[record.key] = offset
		offset += int64(n)
	}

	return seg, nil
}

func (db *Db) recover() error {
	f, err := os.Open(db.outPath)
	if err != nil {
		return err
	}
	defer f.Close()

	in := bufio.NewReader(f)
	for {
		var record entry
		n, err := record.DecodeFromReader(in)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}

		db.index[record.key] = db.outOffset
		db.outOffset += int64(n)
	}
	return nil
}

func (db *Db) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.out.Close()
}

func (db *Db) Get(key string) (string, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if position, ok := db.index[key]; ok {
		value, err := db.readFromFile(db.outPath, position)
		if err == nil {
			return value, nil
		}
	}

	for i := len(db.segments) - 1; i >= 0; i-- {
		seg := db.segments[i]
		if position, ok := seg.index[key]; ok {
			value, err := db.readFromFile(seg.path, position)
			if err == nil {
				return value, nil
			}
		}
	}

	return "", ErrNotFound
}

func (db *Db) readFromFile(path string, position int64) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = file.Seek(position, 0)
	if err != nil {
		return "", err
	}

	var record entry
	if _, err = record.DecodeFromReader(bufio.NewReader(file)); err != nil {
		return "", err
	}
	return record.value, nil
}

func (db *Db) Put(key, value string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	e := entry{
		key:   key,
		value: value,
	}
	data := e.Encode()

	size, err := db.Size()
	if err != nil {
		return err
	}

	if size+int64(len(data)) > db.maxSize {
		if err := db.rotateFile(); err != nil {
			return err
		}
	}

	n, err := db.out.Write(data)
	if err == nil {
		db.index[key] = db.outOffset
		db.outOffset += int64(n)
	}
	return err
}

func (db *Db) rotateFile() error {
	if err := db.out.Close(); err != nil {
		return err
	}

	segmentPath := filepath.Join(db.dir, fmt.Sprintf("%s%d", segmentPrefix, time.Now().UnixNano()))
	if err := os.Rename(db.outPath, segmentPath); err != nil {
		return err
	}

	seg, err := db.loadSegment(segmentPath)
	if err != nil {
		return err
	}
	db.segments = append(db.segments, seg)

	f, err := os.OpenFile(db.outPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}

	db.out = f
	db.outOffset = 0
	db.index = make(hashIndex)
	return nil
}

func (db *Db) Size() (int64, error) {
	info, err := db.out.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (db *Db) MergeSegments() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(db.segments) < 2 {
		return nil
	}

	tempPath := filepath.Join(db.dir, "merged-temp")
	tempFile, err := os.OpenFile(tempPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}

	mergedIndex := make(hashIndex)
	var offset int64

	for _, seg := range db.segments {
		file, err := os.Open(seg.path)
		if err != nil {
			tempFile.Close()
			os.Remove(tempPath)
			return err
		}

		reader := bufio.NewReader(file)
		for {
			var record entry
			_, err := record.DecodeFromReader(reader)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				file.Close()
				tempFile.Close()
				os.Remove(tempPath)
				return err
			}

			if _, exists := mergedIndex[record.key]; !exists {
				data := record.Encode()
				written, err := tempFile.Write(data)
				if err != nil {
					file.Close()
					tempFile.Close()
					os.Remove(tempPath)
					return err
				}
				mergedIndex[record.key] = offset
				offset += int64(written)
			}
		}
		file.Close()
	}

	if err := tempFile.Close(); err != nil {
		os.Remove(tempPath)
		return err
	}

	for _, seg := range db.segments {
		if err := os.Remove(seg.path); err != nil {
			os.Remove(tempPath)
			return err
		}
	}

	newSegmentPath := filepath.Join(db.dir, fmt.Sprintf("%s%d", segmentPrefix, time.Now().UnixNano()))
	if err := os.Rename(tempPath, newSegmentPath); err != nil {
		return err
	}

	newSeg, err := db.loadSegment(newSegmentPath)
	if err != nil {
		return err
	}

	db.segments = []*Segment{newSeg}
	return nil
}