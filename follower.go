package main

import (
	"fmt"
	"strings"
)

type Follower struct {
	nextIndex      map[int]int
	matchIndex     map[int]int
	equivocationDB *EquivocationDB
}

func NewFollower(nodes []string) *Follower {
	return &Follower{
		nextIndex:      map[int]int{},
		matchIndex:     map[int]int{},
		equivocationDB: NewEquivocationDB(nodes),
	}
}

func (f *Follower) BecomeCandidate(id string, currentTerm int, lastLogTerm int, lastLogIndex int) map[string]interface{} {
	msg := map[string]interface{}{
		"type":           "request_vote",
		"term":           currentTerm,
		"candidate_id":   id,
		"last_log_index": lastLogIndex,
		"last_log_term":  lastLogTerm,
	}
	return msg
}

func (f *Follower) AppendEntries(s *Server, msg AppendEntriesRequest) map[string]interface{} {
	response := make(map[string]interface{})
	response["reset_timeout"] = 0
	response["term"] = s.currentTerm

	// Check if leader is banned
	if f.equivocationDB != nil && f.equivocationDB.IsLeaderBanned(msg.LeaderID) {
		println("\033[33m[DEFENSE] Node", s.id, "rejected AppendEntries from banned leader", msg.LeaderID, "\033[0m")
		response["success"] = false
		return response
	}

	// Reject messages from past terms
	if msg.Term < s.currentTerm {
		println("\033[33m[DEFENSE] Node", s.id, "rejected AppendEntries term", msg.Term, "from", msg.LeaderID, "\033[0m")
		response["term"] = s.currentTerm
		response["success"] = false
		return response
	}

	// Leaders shouldn't receive AppendEntries
	if s.currentState == LEADER {
		println("\033[33m[DEFENSE] Node", s.id, "rejected suspicious AppendEntries term", msg.Term, "from", msg.LeaderID, "\033[0m")
		response["term"] = s.currentTerm
		response["success"] = false
		return response
	}

	// Update server state
	s.currentTerm = msg.Term
	s.votedFor = ""
	s.currentState = FOLLOWER
	s.leaderId = msg.LeaderID
	s.resetElectionTimeout()

	// Handle ping messages
	if len(msg.Entries) == 0 {
		response["success"] = true
		response["term"] = s.currentTerm
		response["reset_timeout"] = 1
		return response
	}

	// Check log consistency
	if msg.PrevLogIndex >= len(s.log) ||
		(msg.PrevLogIndex >= 0 && s.log[msg.PrevLogIndex].Term != msg.PrevLogTerm) {
		response["term"] = s.currentTerm
		response["success"] = false
		return response
	}

	println("Follower", s.id, "received AppendEntries from leader", msg.LeaderID, "with term", msg.Term)

	// Process entries, if any suspicious entries are detected, generate an alert and wait for confirmation
	success := true

	for _, entry := range msg.Entries {
		// Check for conflicts in committed entries
		if entry.Index <= s.commitIndex && entry.Index < len(s.log) && s.log[entry.Index].Command != entry.Command {
			success = false
			println("\033[31m[DEFENSE] Node", s.id, "detected suspicious entry at index", entry.Index, "\033[0m")
			// Generate and broadcast alert
			alert := NewEquivocationAlert(msg.LeaderID, entry.Index, entry.Term, s.log[entry.Index], entry, msg.Entries)
			alertMsg := alert.ToRPCMessage()
			key := fmt.Sprintf("%s:%d:%d", msg.LeaderID, msg.PrevLogIndex, msg.PrevLogTerm)
			s.pendingAppendEntries[key] = msg
			broadcast(s, alertMsg, nil)

			// stall the whole process and wait
			response["wait"] = true
			return response
		}
	}

	if success {
		s.log = append(s.log, msg.Entries...)
	}

	// Update commit index only if no suspicious entries detected
	if success && msg.LeaderCommit > s.commitIndex {
		s.commitIndex = min(msg.LeaderCommit, len(s.log)-1)
	}

	// Apply committed entries to state machine
	for i := s.lastApplied + 1; i <= s.commitIndex; i++ {
		entry := s.log[i]
		parts := strings.Split(entry.Command, " ")
		if parts[0] == "write" && len(parts) == 3 {
			s.stateMachine.kv[parts[1]] = parts[2]
		} else if parts[0] == "cas" && len(parts) == 4 {
			key := parts[1]
			from := parts[2]
			to := parts[3]
			currentValue, exists := s.stateMachine.kv[key]
			if exists && currentValue == from {
				s.stateMachine.kv[key] = to
			}
		}
		s.lastApplied = i
	}

	response["success"] = success
	return response
}

func (f *Follower) Vote(s *Server, msg RequestVoteRequest) map[string]interface{} {
	response := make(map[string]interface{})
	if msg.Term < s.currentTerm {
		response["term"] = s.currentTerm
		response["vote_granted"] = false
		return response
	}

	if f.equivocationDB.IsLeaderBanned(msg.CandidateID) {
		println("\033[33m[DEFENSE] Node", s.id, "rejected vote from banned candidate", msg.CandidateID, "\033[0m")
		response["term"] = s.currentTerm
		response["vote_granted"] = false
		return response
	}

	if msg.Term > s.currentTerm {
		s.currentTerm = msg.Term
		s.votedFor = ""
		s.currentState = FOLLOWER
	}
	if s.votedFor == "" || s.votedFor == msg.CandidateID {
		lastLogIndex := len(s.log) - 1
		lastLogTerm := 0
		if lastLogIndex >= 0 {
			lastLogTerm = s.log[lastLogIndex].Term
		}
		if msg.LastLogTerm > lastLogTerm || (msg.LastLogTerm == lastLogTerm && msg.LastLogIndex >= lastLogIndex) {
			s.votedFor = msg.CandidateID
			response["vote_granted"] = true
			s.resetElectionTimeout()
		} else {
			response["vote_granted"] = false
		}
	} else {
		response["vote_granted"] = false
	}
	response["term"] = s.currentTerm
	return response
}

// Utility function to find minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
