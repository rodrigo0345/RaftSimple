package main

import (
	"fmt"
	"strconv"
	"strings"
)

type Leader struct {
	// for each node there is a nextIndex
	nextIndex  map[string]int
	matchIndex map[string]int
}

func NewLeader() *Leader {
	return &Leader{
		nextIndex:  map[string]int{},
		matchIndex: map[string]int{},
	}
}

func (l *Leader) GetHeartbeatMessage(s *Server, id string) map[string]interface{} {
	// Always use -1 for prev log term in heartbeat to avoid conflicts
	prevLogIndex := -1
	prevLogTerm := -1
	if len(s.log) > 0 {
		prevLogIndex = len(s.log) - 1
		prevLogTerm = s.log[prevLogIndex].Term
	}

	return map[string]interface{}{
		"type":           "append_entries",
		"leader_id":      id,
		"term":           s.currentTerm,
		"prev_log_index": prevLogIndex,
		"prev_log_term":  prevLogTerm,
		"leader_commit":  s.commitIndex,
		"entries":        []LogEntry{},
	}
}

func (l *Leader) Write(s *Server, key string, value string, clientMessage *MessageInternal, msgFrom string) (MessageType, *AppendEntriesRequest, string) {

	entry := LogEntry{
		Term:        s.currentTerm,
		Index:       len(s.log),
		Command:     "write " + key + " " + value,
		Message:     clientMessage,
		MessageFrom: msgFrom,
	}
	s.log = append(s.log, entry)

	prevLogIndex := len(s.log) - 2
	prevLogTerm := -1
	if prevLogIndex >= 0 {
		prevLogTerm = s.log[prevLogIndex].Term
	}

	return WRITE_OK, &AppendEntriesRequest{
		Term:         s.currentTerm,
		LeaderID:     s.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      []LogEntry{entry},
		LeaderCommit: s.commitIndex,
	}, ""
}

func (l *Leader) Read(s *Server, key string, clientMessage *MessageInternal, msgFrom string) (MessageType, *AppendEntriesRequest, string) {
	// Create the read log entry without checking the current value
	// The check will be done when the entry is applied
	entry := LogEntry{
		Term:        s.currentTerm,
		Index:       len(s.log),
		Command:     "noop " + key,
		Message:     clientMessage,
		MessageFrom: msgFrom,
	}
	s.log = append(s.log, entry)

	prevLogIndex := len(s.log) - 2
	prevLogTerm := -1
	if prevLogIndex >= 0 {
		prevLogTerm = s.log[prevLogIndex].Term
	}

	return READ_OK, &AppendEntriesRequest{
		Term:         s.currentTerm,
		LeaderID:     s.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      []LogEntry{entry},
		LeaderCommit: s.commitIndex,
	}, ""
}

func (l *Leader) Cas(s *Server, key string, from string, to string, message *MessageInternal, msgFrom string) (MessageType, *AppendEntriesRequest, string) {
	// Create the CAS log entry without checking the current value
	// The check will be done when the entry is applied

	entry := LogEntry{
		Term:        s.currentTerm,
		Index:       len(s.log),
		Command:     "cas " + key + " " + from + " " + to,
		Message:     message,
		MessageFrom: msgFrom,
	}
	s.log = append(s.log, entry)

	prevLogIndex := len(s.log) - 2
	prevLogTerm := -1
	if prevLogIndex >= 0 {
		prevLogTerm = s.log[prevLogIndex].Term
	}

	return CAS_OK, &AppendEntriesRequest{
		Term:         s.currentTerm,
		LeaderID:     s.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      []LogEntry{entry},
		LeaderCommit: s.commitIndex,
	}, ""
}

func (l *Leader) WaitForReplication(s *Server, followerID string, success bool, term int) (MessageType, *AppendEntriesRequest, []ConfirmedOperation) {
	// If the leader receives a term higher than its current term, step down
	if term > s.currentTerm {
		s.currentTerm = term
		s.currentState = FOLLOWER
		s.leaderId = ""
		s.votedFor = ""
		return ERROR, nil, nil
	}

	// só deixa de responder
	if s.id == "n2" {
		return ERROR, nil, nil
	}

	// If the server is no longer the leader, return appropriate message
	if s.currentState != LEADER {
		return NOT_LEADER, nil, nil
	}

	// Handle unsuccessful AppendEntries response (log inconsistency)
	if !success {
		// Decrement nextIndex for the follower and retry
		if l.nextIndex[followerID] > 0 {
			l.nextIndex[followerID]--
		}

		nextIdx := l.nextIndex[followerID]
		prevLogIndex := nextIdx - 1
		prevLogTerm := -1
		if prevLogIndex >= 0 && prevLogIndex < len(s.log) {
			prevLogTerm = s.log[prevLogIndex].Term
		}

		entries := make([]LogEntry, 0)
		if nextIdx < len(s.log) {
			entries = append(entries, s.log[nextIdx])
		}

		retryRequest := &AppendEntriesRequest{
			Term:         s.currentTerm,
			LeaderID:     s.id,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: s.commitIndex,
		}
		return RETRY_SEND, retryRequest, nil
	}

	// Update match and next indices for the follower
	if _, exists := l.nextIndex[followerID]; !exists {
		l.nextIndex[followerID] = 0
	}
	if _, exists := l.matchIndex[followerID]; !exists {
		l.matchIndex[followerID] = 0
	}

	// The follower successfully replicated up to this index
	lastReplicatedIndex := len(s.log) - 1
	if lastReplicatedIndex > l.matchIndex[followerID] {
		l.matchIndex[followerID] = lastReplicatedIndex
		l.nextIndex[followerID] = lastReplicatedIndex + 1
	}

	// Determine new commit index
	newCommitIndex := s.commitIndex
	for index := s.commitIndex + 1; index <= len(s.log)-1; index++ {
		if s.log[index].Term != s.currentTerm {
			continue
		}

		count := 1 // Include leader's own log
		for fid, matchIdx := range l.matchIndex {
			if fid != s.id && matchIdx >= index {
				count++
			}
		}

		if count >= s.majority {
			newCommitIndex = index
		} else {
			break
		}
	}

	// Apply entries and collect confirmed operations
	var confirmedOps []ConfirmedOperation
	if newCommitIndex > s.commitIndex {
		s.commitIndex = newCommitIndex

		// Apply all committed entries that haven't been applied yet
		for i := s.lastApplied + 1; i <= s.commitIndex; i++ {
			entry := s.log[i]
			parts := strings.Split(entry.Command, " ")
			var response map[string]interface{}
			var messageType Operation

			switch parts[0] {
			case "write":
				if len(parts) >= 3 {
					key := parts[1]
					value := parts[2]
					s.stateMachine.kv[key] = value
					messageType = WRITE
					response = map[string]interface{}{
						"type": "write_ok",
					}
				}

			case "noop":
				if len(parts) >= 2 {
					key := parts[1]
					var value string
					var exists bool

					value, exists = s.stateMachine.kv[key]
					if exists {
						valueInt, _ := strconv.Atoi(value)
						response = map[string]interface{}{
							"type":  "read_ok",
							"value": valueInt,
						}
					} else {
						response = map[string]interface{}{
							"type":  "read_ok",
							"value": nil,
						}
					}
					messageType = READ
				}
				break

			case "cas":
				if len(parts) >= 4 {
					key := parts[1]
					from := parts[2]
					to := parts[3]
					currentValue, exists := s.stateMachine.kv[key]

					if !exists {
						messageType = CAS_INVALID_KEY
						response = map[string]interface{}{
							"type": "error",
							"code": 20,
							"text": "CAS target key does not exist",
						}
					} else if currentValue != from {
						messageType = CAS_INVALID_FROM
						displayMessage := fmt.Sprintf("CAS target key does not match `from` in CAS: %s != %s", currentValue, from)
						response = map[string]interface{}{
							"type": "error",
							"code": 22,
							"text": displayMessage,
						}
					} else {
						s.stateMachine.kv[key] = to
						messageType = CAS
						response = map[string]interface{}{
							"type": "cas_ok",
						}
						success = true
					}
				}
			}

			// Record the confirmed operation if we have a client message to respond to
			if response != nil && entry.Message != nil {
				confirmedOp := ConfirmedOperation{
					ClientMessage: entry.Message,
					MessageFrom:   entry.MessageFrom,
					MessageType:   messageType,
					Response:      response,
				}
				confirmedOps = append(confirmedOps, confirmedOp)
			}
			s.lastApplied = i
		}
	}

	return WRITE_OK, nil, confirmedOps
}
