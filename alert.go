package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
)

// PendingEntry holds an alert and the set of nodes that confirmed it
type PendingEntry struct {
	alert       EquivocationAlert
	confirmedBy map[string]bool
	rejectedBy  map[string]bool
}

// EquivocationDB manages equivocation alerts and their confirmations
type EquivocationDB struct {
	pendingEntries map[string]PendingEntry // Key is leader:index:term:localHash:remoteHash
	bannedLeaders  map[string]bool
	majority       int // Number of nodes required for a majority
}

// NewEquivocationDB initializes the EquivocationDB with a majority count
func NewEquivocationDB(nodes []string) *EquivocationDB {
	majority := len(nodes)/2 + 1
	return &EquivocationDB{
		pendingEntries: make(map[string]PendingEntry),
		bannedLeaders:  make(map[string]bool),
		majority:       majority,
	}
}

// EquivocationAlert represents a detected equivocation by the leader
type EquivocationAlert struct {
	LeaderID        string
	Index           int
	Term            int
	LocalHash       string
	RemoteHash      string
	LocalSignature  string
	RemoteSignature string
	PendingEntries  []LogEntry
	Command         string
}

// ProcessAlert validates an alert and updates confirmation counts
// ta tudo mal
func (edb *EquivocationDB) ProcessAlert(alert EquivocationAlert, senderID string) map[string]interface{} {
	// Generate a unique key including both hashes to identify this specific conflict
	key := alert.LeaderID + ":" +
		strconv.Itoa(alert.Index) + ":" +
		strconv.Itoa(alert.Term) + ":" +
		alert.LocalHash + ":" +
		alert.RemoteHash

	// Validate the alert
	if err := ValidateAlert(alert); err != nil {
		edb.pendingEntries[key].rejectedBy[senderID] = true
	}

	// If the alert doesn't exist, initialize it
	if _, exists := edb.pendingEntries[key]; !exists {
		edb.pendingEntries[key] = PendingEntry{
			alert:       alert,
			confirmedBy: make(map[string]bool),
			rejectedBy:  make(map[string]bool),
		}
	}

	// Add the sender to the confirmation set if not already present
	if !edb.pendingEntries[key].confirmedBy[senderID] {
		edb.pendingEntries[key].confirmedBy[senderID] = true
		// Check if the number of confirmations has reached the majority
		if len(edb.pendingEntries[key].confirmedBy) >= edb.majority {
			println("Leader", alert.LeaderID, "is suspicious for index", alert.Index, "term", alert.Term)
			// Here, the system can start ignoring the leader or trigger a new election
			return map[string]interface{}{}
		}
	}
	return map[string]interface{}{}
}

func (edb *EquivocationDB) BanLeader(leaderId string) {
	if edb == nil {
		return
	}
	if _, exists := edb.bannedLeaders[leaderId]; !exists {
		edb.bannedLeaders[leaderId] = true
	}
}

func (edb *EquivocationDB) IsLeaderBanned(leaderId string) bool {
	if edb == nil {
		return false
	}
	for id := range edb.bannedLeaders {
		if id == leaderId {
			return true
		}
	}
	return false
}

// NewEquivocationAlertIfApplicable checks for equivocation in log entries
func (edb *EquivocationDB) NewEquivocationAlertIfApplicable(entry LogEntry, leaderID string, s *Server, pendingEntries []LogEntry) (*EquivocationAlert, error) {
	if entry.Index < len(s.log) {
		existing := s.log[entry.Index]
		if existing.Term == entry.Term && existing.HashEntry() != entry.HashEntry() {
			nea := NewEquivocationAlert(leaderID, entry.Index, entry.Term, existing, entry, pendingEntries)
			return &nea, nil
		}
	}
	return nil, nil
}

// NewEquivocationAlert creates an alert from conflicting entries
func NewEquivocationAlert(leaderID string, idx, term int, local, remote LogEntry, pending []LogEntry) EquivocationAlert {
	return EquivocationAlert{
		LeaderID:        leaderID,
		Index:           idx,
		Term:            term,
		LocalHash:       hashEntry(local),
		RemoteHash:      hashEntry(remote),
		LocalSignature:  local.Signature,
		RemoteSignature: remote.Signature,
		PendingEntries:  pending,
		Command:         remote.Command,
	}
}

// hashEntry computes the hash of a log entry's command and term
func hashEntry(e LogEntry) string {
	h := sha256.New()
	h.Write([]byte(e.Command))
	h.Write([]byte(strconv.Itoa(e.Term)))
	return hex.EncodeToString(h.Sum(nil))
}

// ToRPCMessage converts the alert to a generic format for sending
func (a EquivocationAlert) ToRPCMessage() map[string]interface{} {
	return map[string]interface{}{
		"type":       "equivocation_alert",
		"leader":     a.LeaderID,
		"index":      a.Index,
		"term":       a.Term,
		"hashes":     []string{a.LocalHash, a.RemoteHash},
		"signatures": []string{a.LocalSignature, a.RemoteSignature},
		"command":    a.Command,
	}
}

// ValidateAlert checks hash consistency and signature presence
func ValidateAlert(a EquivocationAlert) error {
	if a.LocalHash == a.RemoteHash {
		return errors.New("no conflict: identical hashes")
	}
	// In a real system, verify signatures; here, just check presence
	if a.LocalSignature == "" || a.RemoteSignature == "" {
		return errors.New("missing signature in at least one version")
	}
	return nil
}

// CountKey returns a unique key for tracking alerts
func (a EquivocationAlert) CountKey() string {
	return a.LeaderID + ":" +
		strconv.Itoa(a.Index) + ":" +
		strconv.Itoa(a.Term)
}
