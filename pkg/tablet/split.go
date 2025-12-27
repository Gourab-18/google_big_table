package tablet

import (
	"fmt"
	"path/filepath"
	"sort"
)

// Split splits the tablet into two new tablets at the median row key.
// returns (leftTablet, rightTablet, error).
// If the tablet is too small to split or has no specific data to determine a midpoint, writes an error.
func (t *Tablet) Split(thresholdBytes int64) (*Tablet, *Tablet, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// 1. Check size:
	// We check MemTable size only for now, as that's what we track easily in this prototype.
	// In a real system, we'd sum SSTable sizes too.
	currentSize := t.MemTable.SizeBytes
	// Also assume 0 for SSTable sizes unless we parse file stats (out of scope for quick impl).
	
	if currentSize < thresholdBytes {
		return nil, nil, fmt.Errorf("tablet size %d is below threshold %d", currentSize, thresholdBytes)
	}

	// 2. Prepare to Snapshot/Compact for Split
	// For simplicity, we flush the memtable first so we have everything in SSTables (or we could keep in MemTable).
	// Let's Flush to unify data.
	sstPath := filepath.Join(t.Dir, fmt.Sprintf("split_temp.sst"))
	// Careful: Tablet struct tracks SSTables, but MemTable.Flush just writes to disk and clears Memtree.
	// We'll manually call Flush on MemTable.
	meta, err := t.MemTable.Flush(sstPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to flush for split: %v", err)
	}
	t.SSTables = append(t.SSTables, *meta)

	// 3. Find Median Key
	// Read all rows from all SSTables to find split point.
	// This is expensive but correct for "Basic" implementation.
	var allRows []*Row
	for _, sst := range t.SSTables {
		rows, err := ReadSSTable(sst.Path)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read sst for split: %v", err)
		}
		allRows = append(allRows, rows...)
	}

	if len(allRows) < 2 {
		return nil, nil, fmt.Errorf("not enough rows to split")
	}

	// Sort by key
	sort.Slice(allRows, func(i, j int) bool {
		return allRows[i].Key < allRows[j].Key
	})

	midIdx := len(allRows) / 2
	splitKey := allRows[midIdx].Key

	fmt.Printf("Splitting at key: %s\n", splitKey)

	// 4. Create Sub-Tablets
	// Left: [StartKey, splitKey)
	// Right: [splitKey, EndKey)
	
	dirLeft := filepath.Join(filepath.Dir(t.Dir), fmt.Sprintf("%s_%s", t.StartKey, splitKey))
	dirRight := filepath.Join(filepath.Dir(t.Dir), fmt.Sprintf("%s_%s", splitKey, t.EndKey))
	
	// Create directories if not exist (NewTablet does this)
	// Note: Directory naming is safe only if keys are filesystem-safe. Assuming simple alphanumeric keys for now.

	leftTablet, err := NewTablet(t.StartKey, splitKey, dirLeft)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create left tablet: %v", err)
	}

	rightTablet, err := NewTablet(splitKey, t.EndKey, dirRight)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create right tablet: %v", err)
	}

	// 5. Partition Data
	// For each row, decide where it goes.
	// Since we already have 'allRows' in memory, just apply them to the new tablets' MemTables.
	// In a real system, we'd link SSTables or rewrite them. Rewriting is cleaner here.
	
	for _, row := range allRows {
		// Convert Row back to Mutation-like application or just direct insertion?
		// We don't have a direct "Insert Row" on Tablet, only Mutate.
		// We can cheat and write to MemTable directly or reconstruct mutations.
		// Reconstructing mutations is safer for abstraction.
		mut := rowToMutation(row)
		if row.Key < splitKey {
			leftTablet.Mutate(mut)
		} else {
			rightTablet.Mutate(mut)
		}
	}
	
	// Flush new tablets to disk to persist state immediately? 
	// Not strictly required but good practice.
	
	return leftTablet, rightTablet, nil
}

// rowToMutation is a helper to convert a Row back to a Set mutation for migration.
func rowToMutation(r *Row) *RowMutation {
	m := NewRowMutation(r.Key)
	for _, col := range r.Columns {
		for _, ver := range col.Versions {
			m.AddSet(col.Family, col.Qualifier, ver.Timestamp, ver.Value)
		}
	}
	return m
}
