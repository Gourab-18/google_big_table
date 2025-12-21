package tablet

import (
	"encoding/json"
	"os"
	"sort"
)

// Compact merges multiple SSTable files into a single new SSTable file.
// It removes superseded versions according to basic logic (merging versions).
// For this "Basic" implementation, we load everything into memory.
func Compact(inputPaths []string, outputPath string) error {
	mergedRows := make(map[string]*Row)

	// 1. Load all rows
	for _, path := range inputPaths {
		rows, err := ReadSSTable(path)
		if err != nil {
			return err
		}

		for _, r := range rows {
			existing, ok := mergedRows[r.Key]
			if !ok {
				mergedRows[r.Key] = r
				continue
			}
			// Merge 'r' into 'existing'
			mergeRows(existing, r)
		}
	}

	// 2. Sort keys
	var keys []string
	for k := range mergedRows {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// 3. Write output
	f, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	for _, k := range keys {
		if err := enc.Encode(mergedRows[k]); err != nil {
			return err
		}
	}

	return nil
}

// mergeRows merges 'source' into 'dest'.
// 'dest' is modified in place.
func mergeRows(dest, source *Row) {
	for colKey, sourceCol := range source.Columns {
		destCol, exists := dest.Columns[colKey]
		if !exists {
			// Deep copy to be safe? Or just pointer assign given we are doing GC compaction?
			// Pointer assign is okay for this scope.
			dest.Columns[colKey] = sourceCol
			continue
		}
		// Merge versions
		destCol.Versions = append(destCol.Versions, sourceCol.Versions...)
		
		// Sort versions descending
		sort.Slice(destCol.Versions, func(i, j int) bool {
			return destCol.Versions[i].Timestamp > destCol.Versions[j].Timestamp
		})

		// TODO: De-duplication or retention policies could go here (e.g. keep top 3).
	}
}
