package tabletserver

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"github.com/Gourab-18/google_big_table/pkg/tablet"
)

// TabletServer manages a set of tablets and serves requests.
type TabletServer struct {
	mu      sync.RWMutex
	RootDir string
	Tablets []*tablet.Tablet // Keeping it simple: linear scan for range.
}

// NewTabletServer creates a new TabletServer.
func NewTabletServer(rootDir string) (*TabletServer, error) {
	if err := os.MkdirAll(rootDir, 0755); err != nil {
		return nil, err
	}

	ts := &TabletServer{
		RootDir: rootDir,
		Tablets: make([]*tablet.Tablet, 0),
	}

	// Bootstrap: Load existing tablets from subdirectories
	entries, err := os.ReadDir(rootDir)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			// Assume dir name is like "start_end" or just generic IDs.
			// For this prototype, we'll try to guess ranges or just load them and inspect.
			// Let's assume we store metadata or just re-open.
			// Currently implementation of NewTablet takes start/end as args,
			// so we strictly need to know them *before* opening,
			// OR we store them in a metadata file inside the tablet dir.
			// Let's cheat: we won't implement full specialized recovery of ranges yet.
			// We will assume 1 default tablet if empty, or just manual setup for now.
		}
	}

	// Auto-bootstrap root tablet if no tablets exist
	if len(ts.Tablets) == 0 {
		// Create default root tablet ["", "")
		rootPath := filepath.Join(rootDir, "root_tablet")
		root, err := tablet.NewTablet("", "", rootPath)
		if err != nil {
			return nil, fmt.Errorf("failed to create root tablet: %w", err)
		}
		ts.Tablets = append(ts.Tablets, root)
	}

	return ts, nil
}

// Serve starts the HTTP server.
func (s *TabletServer) Serve(addr string) error {
	http.HandleFunc("/mutate", s.HandleMutate)
	http.HandleFunc("/read", s.HandleRead)
	return http.ListenAndServe(addr, nil)
}

func (s *TabletServer) HandleMutate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var mut struct {
		RowKey string
		Ops    []struct {
			Type      int
			Family    string
			Qualifier string
			Timestamp int64
			Value     []byte
		}
	}

	if err := json.NewDecoder(r.Body).Decode(&mut); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Convert to internal Mutation
	rm := tablet.NewRowMutation(mut.RowKey)
	for _, op := range mut.Ops {
		if op.Type == 0 { // Set, assuming enum matches
			rm.AddSet(op.Family, op.Qualifier, op.Timestamp, op.Value)
		} else {
			rm.AddDelete(op.Family, op.Qualifier)
		}
	}

	// Find Tablet
	t := s.findTablet(rm.RowKey)
	if t == nil {
		http.Error(w, "No tablet found for key", http.StatusInternalServerError)
		return
	}

	if err := t.Mutate(rm); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *TabletServer) HandleRead(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	family := r.URL.Query().Get("family")
	qualifier := r.URL.Query().Get("qualifier")

	if key == "" || family == "" || qualifier == "" {
		http.Error(w, "Missing params", http.StatusBadRequest)
		return
	}

	t := s.findTablet(key)
	if t == nil {
		http.Error(w, "No tablet for key", http.StatusNotFound)
		return
	}

	ver, err := t.Read(key, family, qualifier)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if ver == nil {
		http.Error(w, "Not found", http.StatusNotFound)
		return
	}

	json.NewEncoder(w).Encode(ver)
}

func (s *TabletServer) findTablet(key string) *tablet.Tablet {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, t := range s.Tablets {
		if t.InRange(key) {
			return t
		}
	}
	return nil
}
