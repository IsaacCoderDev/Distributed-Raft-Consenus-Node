package raft

import (
	"encoding/binary"
	"fmt"
	"os"

	pb "github.com/quant/raft-go/proto"
	"google.golang.org/protobuf/proto"
)

type WAL struct {
	file *os.File

	marshalBuf []byte
}

func NewWAL(filePath string) (*WAL, error) {
	flags := os.O_APPEND | os.O_CREATE | os.O_WRONLY | os.O_DSYNC

	f, err := os.OpenFile(filePath, flags, 0644)

	if err != nil {
		return nil, err
	}

	return &WAL{
		file:       f,
		marshalBuf: make([]byte, 0, 4096), // Pre-allocate 4KB
	}, nil
}

func (w *WAL) AppendLog(entry *pb.LogEntry) error {
	w.marshalBuf = w.marshalBuf[:0]

	var err error

	w.marshalBuf, err = proto.MarshalOptions{}.MarshalAppend(w.marshalBuf, entry)

	if err != nil {
		return fmt.Errorf("failed to marshal entry: %w", err)
	}

	sizeHeader := make([]byte, 4)
	binary.LittleEndian.PutUint32(sizeHeader, uint32(len(w.marshalBuf)))

	if _, err := w.file.Write(sizeHeader); err != nil {
		return err
	}

	if _, err := w.file.Write(w.marshalBuf); err != nil {
		return err
	}

	return nil
}

func (w *WAL) PersistState(term uint64, votedFor uint32) error {
	return nil
}

func (w *WAL) Close() error {
	return w.file.Close()
}
