package main

import "strings"

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
	return map[string]interface{}{
		"type":      "append_entries",
		"leader_id": id,                     // Ensure this is set
		"term":      float64(s.currentTerm), // Exam
		// acho que prev_log_index nap interessa
		"prev_log_index": float64(s.lastApplied), // Adjust accordingly
		"prev_log_term":  float64(s.currentTerm), // Adjust accordingly
		"leader_commit":  float64(s.commitIndex), // Adjust accordingly
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

func (l *Leader) Cas(s *Server, key string, from string, to string, message *MessageInternal, msgFrom string) (MessageType, *AppendEntriesRequest, string) {
	// check if the key exists in the current state
	/*
		value, err := s.stateMachine.kv[key]
		if err {
			return CAS_INVALID_KEY, nil, ""
		}
		if value != from {
			return CAS_INVALID_FROM, nil, ""
		}
	*/

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

	// If the server is no longer the leader, return appropriate message
	if s.currentState != LEADER {
		return NOT_LEADER, nil, nil
	}

	// Handle unsuccessful AppendEntries response (log inconsistency)
	if !success {
		if s.leader.nextIndex[followerID] > 1 {
			s.leader.nextIndex[followerID]--
		}
		nextIdx := s.leader.nextIndex[followerID]
		prevLogIndex := nextIdx - 1
		prevLogTerm := -1
		if prevLogIndex >= 0 {
			prevLogTerm = s.log[prevLogIndex].Term
		}

		entries := make([]LogEntry, 0)
		if nextIdx < len(s.log) {
			entries = s.log[nextIdx : nextIdx+1]
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
	lastLogIndex := len(s.log) - 1
	s.leader.matchIndex[followerID] = lastLogIndex
	s.leader.nextIndex[followerID] = lastLogIndex + 1

	// Determine new commit index
	newCommitIndex := s.commitIndex
	for index := s.commitIndex + 1; index <= lastLogIndex; index++ {
		if s.log[index].Term != s.currentTerm {
			continue
		}

		count := 1 // Include leader's own log
		for fid, matchIdx := range s.leader.matchIndex {
			if fid != s.id && matchIdx >= index {
				count++
			}
		}

		if count >= s.majority {
			newCommitIndex = index
		} else {
			break // No higher index can be committed
		}
	}

	// Update commit index if new higher index is found
	if newCommitIndex > s.commitIndex {
		s.commitIndex = newCommitIndex
	}

	// Apply entries and collect confirmed operations
	var confirmedOps []ConfirmedOperation
	if s.commitIndex > s.lastApplied {
		for i := s.lastApplied + 1; i <= s.commitIndex; i++ {
			entry := s.log[i]
			parts := strings.Split(entry.Command, " ")
			var response map[string]interface{}
			var messageType Operation

			switch parts[0] {
			case "write":
				if len(parts) == 3 {
					key := parts[1]
					value := parts[2]
					s.stateMachine.kv[key] = value
					messageType = WRITE
					response = map[string]interface{}{
						"type": "write_ok",
					}
				}
			case "cas":
				if len(parts) == 4 {
					key := parts[1]
					from := parts[2]
					to := parts[3]
					currentValue, exists := s.stateMachine.kv[key]

					if !exists {
						messageType = CAS_INVALID_KEY
						response = map[string]interface{}{
							"type":  "error",
							"value": 20,
						}
					} else if currentValue != from {
						messageType = CAS_INVALID_FROM
						response = map[string]interface{}{
							"type":  "error",
							"value": 22,
						}
					} else {
						s.stateMachine.kv[key] = to
						messageType = CAS
						response = map[string]interface{}{
							"type": "cas_ok",
						}
					}
				}
			}

			// Record the confirmed operation
			if response != nil {
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
