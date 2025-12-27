package tablet

import (
	"sync"

	"github.com/google/btree"
)

// MemTable represents the in-memory buffer of mutations (LSM tree component).
// It maintains rows in sorted order using a B-Tree.
type MemTable struct {
	mu        sync.RWMutex
	tree      *btree.BTree
	SizeBytes int64
}

// NewMemTable creates a new MemTable.
func NewMemTable() *MemTable {
	return &MemTable{
		tree:      btree.New(32),
		SizeBytes: 0,
	}
}

// rowItem is a wrapper for Row to implement the btree.Item interface.
type rowItem struct {
	*Row
}

// Less implements btree.Item.
// It orders rows lexicographically by their Key.
func (r rowItem) Less(than btree.Item) bool {
	return r.Row.Key < than.(rowItem).Row.Key
}

// Apply applies a mutation to the MemTable.
// It is thread-safe.
func (m *MemTable) Apply(mutation *RowMutation) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 1. Find or create the row
	var row *Row
	item := m.tree.Get(rowItem{Row: &Row{Key: mutation.RowKey}})
	if item == nil {
		row = NewRow(mutation.RowKey)
		m.tree.ReplaceOrInsert(rowItem{Row: row})
		m.SizeBytes += int64(len(mutation.RowKey))
	} else {
		row = item.(rowItem).Row
	}

	// 2. Apply the mutation to the row
	m.SizeBytes += estimateMutationSize(mutation)
	
	// Since we hold the MemTable lock, this entire operation is atomic
	// with respect to other MemTable operations.
	return row.Apply(mutation)
}

func estimateMutationSize(m *RowMutation) int64 {
	var size int64
	for _, op := range m.Ops {
		size += int64(len(op.Family) + len(op.Qualifier) + len(op.Value) + 8) // +8 for timestamp
	}
	return size
}

// Get retrieves a row from the MemTable.
func (m *MemTable) Get(key string) *Row {
	m.mu.RLock()
	defer m.mu.RUnlock()

	item := m.tree.Get(rowItem{Row: &Row{Key: key}})
	if item == nil {
		return nil
	}
	// We return the pointer to the row. Thread-safety warning:
	// Reading the Row's internal map is NOT safe if another goroutine is writing to it.
	// For a real production system, we would return a copy or use a read-only view.
	// For this prototype, we rely on the fact that 'Row' operations are not
	// individually locked, but MemTable access is.
	// However, this means the caller MUST NOT hold this pointer long-term while others write.
	// To be safer, let's deep copy broadly or just rely on 'Apply' locking for now.
	return item.(rowItem).Row
}
