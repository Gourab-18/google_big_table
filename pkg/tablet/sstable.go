package tablet

import (
	"encoding/json"
	"io"
	"os"

	"github.com/google/btree"
)

// SSTableMetadata represents an SSTable on disk
type SSTableMetadata struct {
	Path string
}

// FlushMemTable writes the current MemTable to an SSTable file and clears the MemTable.
func (m *MemTable) Flush(path string) (*SSTableMetadata, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	enc := json.NewEncoder(f)

	// Iterate over the BTree and write rows
	// btree.Ascend is in-order traversal (sorted by key)
	var writeErr error
	m.tree.Ascend(func(i btree.Item) bool {
		row := i.(rowItem).Row
		if err := enc.Encode(row); err != nil {
			writeErr = err
			return false // stop iteration
		}
		return true
	})

	if writeErr != nil {
		return nil, writeErr
	}

	// Clear MemTable
	m.tree.Clear(false)

	return &SSTableMetadata{Path: path}, nil
}

// ReadSSTable reads all rows from an SSTable file.
// In a real system, we would have an index or scan iterator.
// For this prototype, we load all into memory for simplicity in Compaction.
func ReadSSTable(path string) ([]*Row, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var rows []*Row
	dec := json.NewDecoder(f)
	for {
		var r Row
		if err := dec.Decode(&r); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		// dec.Decode allocates new underlying arrays for slices if we pointer correctly,
		// but since 'Versions' is a slice, we must be careful.
		// Use a fresh struct each time to avoid sharing underlying arrays if decoder reuses (it typically doesn't for structs).
		rows = append(rows, &r)
	}
	return rows, nil
}
