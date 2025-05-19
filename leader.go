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
	attack     *Attack
}

func NewLeader() *Leader {
	return &Leader{
		nextIndex:  map[string]int{},
		matchIndex: map[string]int{},
		attack:     NewAttack(), // disabled by default
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

	prevLogIndex := len(s.log) - 2 // Index of entry before the new one
	prevLogTerm := -1
	if prevLogIndex >= 0 {
		prevLogTerm = s.log[prevLogIndex].Term
	}
	apr := AppendEntriesRequest{
		Term:         s.currentTerm,
		LeaderID:     s.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      []LogEntry{entry}, // Send the new entry
		LeaderCommit: s.commitIndex,
	}
	apr = l.attack.GenerateAppendEntriesRequest(s, apr)

	return WRITE_OK, &apr, ""
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

	apr := AppendEntriesRequest{
		Term:         s.currentTerm,
		LeaderID:     s.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      []LogEntry{entry}, // Send the new entry
		LeaderCommit: s.commitIndex,
	}
	apr = l.attack.GenerateAppendEntriesRequest(s, apr)

	return READ_OK, &apr, ""
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

	prevLogIndex := len(s.log) - 2 // Index of entry before the new one
	prevLogTerm := -1
	if prevLogIndex >= 0 {
		prevLogTerm = s.log[prevLogIndex].Term
	}

	apr := AppendEntriesRequest{
		IsAttack:     false,
		Term:         s.currentTerm,
		LeaderID:     s.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      []LogEntry{entry}, // Send the new entry
		LeaderCommit: s.commitIndex,
	}
	apr = l.attack.GenerateAppendEntriesRequest(s, apr)

	return CAS_OK, &apr, ""
}

func (l *Leader) WaitForReplication(s *Server, followerID string, success bool, term int) (MessageType, *AppendEntriesRequest, []ConfirmedOperation) {
	println("Entering WaitForReplication for follower:", followerID, "success:", success, "term:", term, "leader term:", s.currentTerm)

	if s.currentState != LEADER {
		println("Not the leader, exiting WaitForReplication")
		return NOT_LEADER, nil, nil
	}
	println("Server is leader, proceeding with replication check")

	if term > s.currentTerm {
		println("Higher term detected from follower:", followerID, "new term:", term, "old term:", s.currentTerm)
		s.currentTerm = term
		s.currentState = FOLLOWER
		s.leaderId = ""
		s.votedFor = ""
		l.nextIndex = make(map[string]int)
		l.matchIndex = make(map[string]int)
		println("Leader stepping down, new state: FOLLOWER")
		return ERROR, nil, nil
	}
	println("No term conflict, leader term is current or higher")

	if !success {
		println("AppendEntries failed for follower:", followerID)
		if l.nextIndex[followerID] > 0 {
			l.nextIndex[followerID]--
			println("Decremented nextIndex for follower:", followerID, "new value:", l.nextIndex[followerID])
		} else {
			println("nextIndex for follower:", followerID, "is already 0, no decrement")
		}

		nextIdxToSend := l.nextIndex[followerID]
		prevLogIndexForRetry := nextIdxToSend - 1
		prevLogTermForRetry := -1
		println("Preparing retry: nextIdxToSend:", nextIdxToSend, "prevLogIndexForRetry:", prevLogIndexForRetry)

		if prevLogIndexForRetry >= 0 && prevLogIndexForRetry < len(s.log) {
			prevLogTermForRetry = s.log[prevLogIndexForRetry].Term
			println("Set prevLogTermForRetry to:", prevLogTermForRetry, "from log index:", prevLogIndexForRetry)
		} else {
			println("prevLogIndexForRetry out of bounds, using prevLogTermForRetry:", prevLogTermForRetry)
		}

		entriesToRetry := make([]LogEntry, 0)
		if nextIdxToSend >= 0 && nextIdxToSend < len(s.log) {
			entriesToRetry = append(entriesToRetry, s.log[nextIdxToSend])
			println("Added entry to retry at index:", nextIdxToSend)
		} else {
			println("No entries to retry, nextIdxToSend:", nextIdxToSend, "log length:", len(s.log))
		}

		retryRequest := &AppendEntriesRequest{
			Term:         s.currentTerm,
			LeaderID:     s.id,
			PrevLogIndex: prevLogIndexForRetry,
			PrevLogTerm:  prevLogTermForRetry,
			Entries:      entriesToRetry,
			LeaderCommit: s.commitIndex,
		}
		println("Returning RETRY_SEND with request for follower:", followerID)
		return RETRY_SEND, retryRequest, nil
	}

	println("AppendEntries succeeded for follower:", followerID)

	if _, ok := l.nextIndex[followerID]; !ok {
		println("nextIndex for follower:", followerID, "not initialized")
		lastLogIdx := -1
		if len(s.log) > 0 {
			lastLogIdx = len(s.log) - 1
		}
		l.nextIndex[followerID] = lastLogIdx + 1
		l.matchIndex[followerID] = -1
		println("Initialized nextIndex[", followerID, "]=", l.nextIndex[followerID], "matchIndex[", followerID, "]=", l.matchIndex[followerID])
	}

	if len(s.log) > 0 {
		newMatchIndex := len(s.log) - 1
		if l.matchIndex[followerID] < newMatchIndex {
			l.matchIndex[followerID] = newMatchIndex
			println("Updated matchIndex[", followerID, "] to", l.matchIndex[followerID])
		} else {
			println("matchIndex[", followerID, "]=", l.matchIndex[followerID], "not updated, already sufficient")
		}
		l.nextIndex[followerID] = l.matchIndex[followerID] + 1
		println("Set nextIndex[", followerID, "]=", l.nextIndex[followerID])
	} else {
		l.matchIndex[followerID] = -1
		l.nextIndex[followerID] = 0
		println("Log empty, set matchIndex[", followerID, "]=", l.matchIndex[followerID], "nextIndex[", followerID, "]=", l.nextIndex[followerID])
	}

	newCommitIndex := s.commitIndex

	for index := s.commitIndex + 1; index < len(s.log); index++ {
		println("Checking commit for index:", index, "term:", s.log[index].Term)
		if s.log[index].Term != s.currentTerm {
			println("Skipping index:", index, "term mismatch with current term:", s.currentTerm)
			continue
		}

		count := 1
		for fID, matchIdx := range l.matchIndex {
			if fID != s.id && matchIdx >= index {
				count++
				println("Follower", fID, "matches index:", index, "count now:", count)
			}
		}

		println("Replication count for index:", index, "is", count, "majority required:", s.majority)
		if count >= s.majority {
			newCommitIndex = index
			println("Index:", index, "can be committed, newCommitIndex:", newCommitIndex)
		} else {
			println("Index:", index, "not replicated by majority, stopping commit check")
			break
		}
	}
	println("Commit check complete, old commitIndex:", s.commitIndex, "new commitIndex:", newCommitIndex)

	var confirmedOps []ConfirmedOperation
	if newCommitIndex > s.commitIndex {
		s.commitIndex = newCommitIndex
		println("Advanced commitIndex to:", s.commitIndex)

		for i := s.lastApplied + 1; i <= s.commitIndex; i++ {
			if i < 0 || i >= len(s.log) {
				println("Error: index", i, "out of bounds, log length:", len(s.log))
				continue
			}

			entry := s.log[i]
			parts := strings.Split(entry.Command, " ")
			var response map[string]interface{}
			var messageType Operation
			println("Applying log[", i, "]:", entry.Command)

			switch parts[0] {
			case "write":
				println("Processing write operation")
				if len(parts) >= 3 {
					key := parts[1]
					value := parts[2]
					s.stateMachine.kv[key] = value
					messageType = WRITE
					response = map[string]interface{}{
						"type": "write_ok",
					}
					println("Write applied: key=", key, "value=", value)
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
				println("Processing CAS operation:", entry.Command)
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
							"text": fmt.Sprintf("CAS failed: key '%s' does not exist", key),
						}
						println("CAS failed: key", key, "does not exist")
					} else if fmt.Sprintf("%v", currentValue) != from {
						messageType = CAS_INVALID_FROM
						displayMessage := fmt.Sprintf("CAS failed for key '%s': current value '%v' does not match 'from' value '%s'", key, currentValue, from)
						response = map[string]interface{}{
							"type": "error",
							"code": 22,
							"text": displayMessage,
						}
						println("CAS failed: key=", key, "current=", currentValue, "expected=", from)
					} else {
						s.stateMachine.kv[key] = to
						messageType = CAS
						response = map[string]interface{}{
							"type": "cas_ok",
						}
						println("CAS succeeded: key=", key, "updated to", to)
					}
				}
			}

			if response != nil && entry.Message != nil {
				println("Operation applied, response type:", response["type"], "for client message")
				confirmedOp := ConfirmedOperation{
					ClientMessage: entry.Message,
					MessageFrom:   entry.MessageFrom,
					MessageType:   messageType,
					Response:      response,
				}
				confirmedOps = append(confirmedOps, confirmedOp)
			} else if entry.Message != nil {
				println("Operation applied but no response generated for command:", entry.Command)
			}
			s.lastApplied = i
			println("Updated lastApplied to:", s.lastApplied)
		}
	}

	println("Exiting WaitForReplication, returning WRITE_OK for follower:", followerID, "with", len(confirmedOps), "confirmed operations")
	return WRITE_OK, nil, confirmedOps
}
