package tablet

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Tablet represents a contiguous range of rows in the table.
// It manages the MemTable, SSTables, and Commit Log for that range.
type Tablet struct {
	mu sync.RWMutex

	StartKey string
	EndKey   string // Exclusive. If empty, it means positive infinity (end of table).

	Dir string // Directory for data storage (WAL, SSTables)

	MemTable  *MemTable
	SSTables  []SSTableMetadata
	CommitLog *CommitLog
}

// NewTablet initializes a new Tablet.
func NewTablet(start, end, dir string) (*Tablet, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	// Initialize Commit Log
	walPath := filepath.Join(dir, "tablet.wal")
	cl, err := NewCommitLog(walPath)
	if err != nil {
		return nil, err
	}

	// TODO: Load existing SSTables and replay WAL on real recovery.
	// For this step, we assume fresh or just minimal init.

	return &Tablet{
		StartKey:  start,
		EndKey:    end,
		Dir:       dir,
		MemTable:  NewMemTable(),
		CommitLog: cl,
		SSTables:  make([]SSTableMetadata, 0),
	}, nil
}

// Mutate applies a mutation to the tablet.
// It verifies the row key is within range, writes to the WAL, and updates the MemTable.
func (t *Tablet) Mutate(m *RowMutation) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.InRange(m.RowKey) {
		return fmt.Errorf("key '%s' out of range [%s, %s)", m.RowKey, t.StartKey, t.EndKey)
	}

	// 1. Write to WAL (Durability)
	if err := t.CommitLog.Append(m); err != nil {
		return fmt.Errorf("failed to append to WAL: %w", err)
	}

	// 2. Update MemTable (Visibility)
	if err := t.MemTable.Apply(m); err != nil {
		return fmt.Errorf("failed to apply to MemTable: %w", err)
	}

	return nil
}

// InRange checks if a key belongs to this tablet.
func (t *Tablet) InRange(key string) bool {
	if key < t.StartKey {
		return false
	}
	if t.EndKey != "" && key >= t.EndKey {
		return false
	}
	return true
}

// Close closes the tablet's resources.
func (t *Tablet) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.CommitLog.Close()
}
