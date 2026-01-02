package tablet

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// CellVersion represents a specific version of a cell's value.
type CellVersion struct {
	Timestamp int64
	Value     []byte
}

// Column represents a column in a row, containing multiple versions of data.
type Column struct {
	Family   string
	Qualifier string
	Versions []CellVersion
}

// NewColumn creates a new column.
func NewColumn(family, qualifier string) *Column {
	return &Column{
		Family:    family,
		Qualifier: qualifier,
		Versions:  make([]CellVersion, 0),
	}
}

// Insert adds a new version to the column.
// It maintains the order of versions by Sort Order: timestamp descending (latest first).
func (c *Column) Insert(timestamp int64, value []byte) {
	// If timestamp is 0, use current time 
	if timestamp == 0 {
		timestamp = time.Now().UnixNano()
	}

	newVer := CellVersion{
		Timestamp: timestamp,
		Value:     value,
	}

	// Find insertion point to keep sorted by Timestamp Descending
	// sort.Search returns the smallest index i in [0, n) at which f(i) is true.
	// We want the first index where Versions[i].Timestamp <= newVer.Timestamp
	// because we want descending order.
	idx := sort.Search(len(c.Versions), func(i int) bool {
		return c.Versions[i].Timestamp <= timestamp
	})

	c.Versions = append(c.Versions, CellVersion{})
	copy(c.Versions[idx+1:], c.Versions[idx:])
	c.Versions[idx] = newVer
}

// GetLatest returns the latest version of the cell data.
func (c *Column) GetLatest() *CellVersion {
	if len(c.Versions) == 0 {
		return nil
	}
	return &c.Versions[0]
}

// Row represents a single row in the table.
type Row struct {
	mu      sync.RWMutex
	Key     string
	Columns map[string]*Column // Key is "Family:Qualifier"
}

// NewRow creates a new row.
func NewRow(key string) *Row {
	return &Row{
		Key:     key,
		Columns: make(map[string]*Column),
	}
}

// Set adds a value to a specific column family and qualifier.
func (r *Row) Set(family, qualifier string, timestamp int64, value []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	colKey := family + ":" + qualifier
	col, exists := r.Columns[colKey]
	if !exists {
		col = NewColumn(family, qualifier)
		r.Columns[colKey] = col
	}
	col.Insert(timestamp, value)
}

// DeleteColumn deletes all data for a specific column.
func (r *Row) DeleteColumn(family, qualifier string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	colKey := family + ":" + qualifier
	delete(r.Columns, colKey)
}

// Get returns the latest value for a specific column.
func (r *Row) Get(family, qualifier string) *CellVersion {
	r.mu.RLock()
	defer r.mu.RUnlock()

	colKey := family + ":" + qualifier
	col, exists := r.Columns[colKey]
	if !exists {
		return nil
	}
	return col.GetLatest()
}

// MutationType defines the type of mutation.
type MutationType int

const (
	MutationSet MutationType = iota
	MutationDelete
)

// MutationOperation represents a single operation within a mutation.
type MutationOperation struct {
	Type      MutationType
	Family    string
	Qualifier string
	Timestamp int64
	Value     []byte
}

// RowMutation represents a set of operations to be applied atomically to a row.
type RowMutation struct {
	RowKey string
	Ops    []MutationOperation
}

// NewRowMutation creates a new RowMutation.
func NewRowMutation(rowKey string) *RowMutation {
	return &RowMutation{
		RowKey: rowKey,
		Ops:    make([]MutationOperation, 0),
	}
}

// AddSet adds a set operation to the mutation.
func (rm *RowMutation) AddSet(family, qualifier string, timestamp int64, value []byte) {
	rm.Ops = append(rm.Ops, MutationOperation{
		Type:      MutationSet,
		Family:    family,
		Qualifier: qualifier,
		Timestamp: timestamp,
		Value:     value,
	})
}

// AddDelete adds a delete operation to the mutation.
func (rm *RowMutation) AddDelete(family, qualifier string) {
	rm.Ops = append(rm.Ops, MutationOperation{
		Type:      MutationDelete,
		Family:    family,
		Qualifier: qualifier,
	})
}

// Apply applies a RowMutation to the row.
// It ensures that the mutation is applied to the correct row and executes all operations.
func (r *Row) Apply(m *RowMutation) error {
	if r.Key != m.RowKey {
		return fmt.Errorf("mutation row key %s does not match row key %s", m.RowKey, r.Key)
	}

	// Lock the row for the entire duration of the mutation batch to ensure atomicity.
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, op := range m.Ops {
		switch op.Type {
		case MutationSet:
			// We duplicate internal Set logic here to avoid recursive locking
			// or we could split Set into locked/unlocked versions.
			// Ideally call unlocked version.
			r.setInternal(op.Family, op.Qualifier, op.Timestamp, op.Value)
		case MutationDelete:
			r.deleteInternal(op.Family, op.Qualifier)
		}
	}
	return nil
}

// setInternal matches Set but assumes lock is held.
func (r *Row) setInternal(family, qualifier string, timestamp int64, value []byte) {
	colKey := family + ":" + qualifier
	col, exists := r.Columns[colKey]
	if !exists {
		col = NewColumn(family, qualifier)
		r.Columns[colKey] = col
	}
	col.Insert(timestamp, value)
}

// deleteInternal matches DeleteColumn but assumes lock is held.
func (r *Row) deleteInternal(family, qualifier string) {
	colKey := family + ":" + qualifier
	delete(r.Columns, colKey)
}
