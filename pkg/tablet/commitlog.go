package tablet

import (
	"encoding/gob"
	"io"
	"os"
	"sync"
)

// CommitLog manages the append-only log file for durability.
type CommitLog struct {
	mu   sync.Mutex
	file *os.File
	enc  *gob.Encoder
	path string
}

// NewCommitLog creates or opens an existing commit log.
func NewCommitLog(path string) (*CommitLog, error) {
	// Open file in append mode, create if not exists
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &CommitLog{
		file: f,
		enc:  gob.NewEncoder(f),
		path: path,
	}, nil
}

// Append writes a mutation to the log.
// It ensures durability by syncing to disk.
func (l *CommitLog) Append(mutation *RowMutation) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Encode and write to file
	if err := l.enc.Encode(mutation); err != nil {
		return err
	}

	// Ensure durability
	return l.file.Sync()
}

// Close closes the log file.
func (l *CommitLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Close()
}

// Recover reads all mutations from the log file.
// This is used to rebuild the MemTable on restart.
func (l *CommitLog) Recover() ([]*RowMutation, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Need to read from the beginning
	// For simplicity, let's open a new reader interface to the same file path
	// because seeking on the append-only writer handle might interact poorly with the encoder state.
	f, err := os.Open(l.path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var mutations []*RowMutation
	dec := gob.NewDecoder(f)

	for {
		var m RowMutation
		err := dec.Decode(&m)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		mutations = append(mutations, &m)
	}

	return mutations, nil
}
