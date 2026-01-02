package master

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
)

// TabletLocation represents where a tablet is currently served.
type TabletLocation struct {
	TabletID string
	StartKey string
	EndKey   string
	ServerID string // Address or ID of TabletServer
}

// Master manages metadata for tablets and tablet servers.
type Master struct {
	mu sync.RWMutex

	// Registry of live tablet servers
	// map[ServerID]LastHeartbeat
	Servers map[string]int64

	// Tablet Assignment Metadata
	// For simplicity, we keep a flat list or map.
	// In Bigtable, this is the META0/META1 table.
	// Here, in-memory map.
	TabletLocations []TabletLocation
}

// NewMaster creates a new Master instance.
func NewMaster() *Master {
	return &Master{
		Servers:         make(map[string]int64),
		TabletLocations: make([]TabletLocation, 0),
	}
}

// Serve starts the Master HTTP server.
func (m *Master) Serve(addr string) error {
	http.HandleFunc("/register", m.HandleRegister)
	http.HandleFunc("/heartbeat", m.HandleHeartbeat)
	http.HandleFunc("/tablets", m.HandleGetTablets)
	http.HandleFunc("/split-report", m.HandleSplitReport)

	return http.ListenAndServe(addr, nil)
}

func (m *Master) HandleRegister(w http.ResponseWriter, r *http.Request) {
	serverID := r.URL.Query().Get("id")
	if serverID == "" {
		http.Error(w, "missing id", http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.Servers[serverID] = 0 // Timestamp to be updated by heartbeat
	fmt.Printf("Registered new tablet server: %s\n", serverID)

	// Initial Assignment: If this is the first server and we have no locations,
	// assign the root tablet to it.
	if len(m.TabletLocations) == 0 {
		m.TabletLocations = append(m.TabletLocations, TabletLocation{
			TabletID: "root",
			StartKey: "",
			EndKey:   "",
			ServerID: serverID,
		})
		fmt.Printf("Assigned root tablet to %s\n", serverID)
	}

	w.WriteHeader(http.StatusOK)
}

func (m *Master) HandleHeartbeat(w http.ResponseWriter, r *http.Request) {
	// TODO: Update timestamp
}

func (m *Master) HandleGetTablets(w http.ResponseWriter, r *http.Request) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	json.NewEncoder(w).Encode(m.TabletLocations)
}

func (m *Master) HandleSplitReport(w http.ResponseWriter, r *http.Request) {
	// Receive report: "Tablet X split into Y (left) and Z (right) at Key K"
	// Master updates metadata map: Remove X, Add Y and Z.
	// Assigns Y and Z to the same server for now (local split), can rebalance later.

	var split struct {
		ParentID string
		Left     TabletLocation
		Right    TabletLocation
	}
	if err := json.NewDecoder(r.Body).Decode(&split); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// 1. Find and remove Parent
	// Inefficient slice removal for prototype
	newLocs := make([]TabletLocation, 0, len(m.TabletLocations)+1)
	found := false
	for _, t := range m.TabletLocations {
		if t.TabletID == split.ParentID {
			found = true
			continue // Skip (remove)
		}
		newLocs = append(newLocs, t)
	}

	if !found {
		// Might have been processed or race condition
		fmt.Printf("Warning: Split parent %s not found in metadata\n", split.ParentID)
	}

	// 2. Add children
	// Assume the server reporting handles them for now
	newLocs = append(newLocs, split.Left, split.Right)

	m.TabletLocations = newLocs
	fmt.Printf("Processed split for %s. New count: %d\n", split.ParentID, len(m.TabletLocations))

	w.WriteHeader(http.StatusOK)
}
