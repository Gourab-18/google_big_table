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

	// Recovery: Load existing SSTables
	var sstables []SSTableMetadata
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, f := range files {
		if filepath.Ext(f.Name()) == ".sst" {
			sstables = append(sstables, SSTableMetadata{Path: filepath.Join(dir, f.Name())})
		}
	}

	t := &Tablet{
		StartKey:  start,
		EndKey:    end,
		Dir:       dir,
		MemTable:  NewMemTable(),
		CommitLog: cl,
		SSTables:  sstables,
	}

	// Recovery: Replay WAL
	mutations, err := cl.Recover()
	if err != nil {
		return nil, fmt.Errorf("failed to recover WAL: %w", err)
	}

	// Replay mutations into MemTable (restore state)
	for _, m := range mutations {
		if err := t.MemTable.Apply(m); err != nil {
			return nil, fmt.Errorf("failed to replay mutation: %w", err)
		}
	}

	return t, nil
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

// Read returns the latest value for a specific column.
// It checks MemTable and all SSTables.
func (t *Tablet) Read(rowKey, family, qualifier string) (*CellVersion, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if !t.InRange(rowKey) {
		return nil, fmt.Errorf("key '%s' out of range [%s, %s)", rowKey, t.StartKey, t.EndKey)
	}

	var candidates []CellVersion

	// 1. Check MemTable
	if row := t.MemTable.Get(rowKey); row != nil {
		if ver := row.Get(family, qualifier); ver != nil {
			candidates = append(candidates, *ver)
		}
	}

	// 2. Check SSTables (Expensive scan)
	for _, sst := range t.SSTables {
		// Optimization: We could keep Bloom Filters or Start/End keys per SSTable
		rows, err := ReadSSTable(sst.Path)
		if err != nil {
			// Log error but maybe continue? failure is safer
			return nil, fmt.Errorf("failed to read sstable %s: %w", sst.Path, err)
		}
		for _, r := range rows {
			if r.Key == rowKey {
				if ver := r.Get(family, qualifier); ver != nil {
					candidates = append(candidates, *ver)
				}
				break // Found row in this SSTable
			}
		}
	}

	if len(candidates) == 0 {
		return nil, nil // Not found
	}

	// 3. Find latest
	var best *CellVersion
	for i := range candidates {
		if best == nil || candidates[i].Timestamp > best.Timestamp {
			best = &candidates[i]
		}
	}

	return best, nil
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
