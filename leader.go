package main

import (
	"fmt"
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

	prevLogIndex := len(s.log) - 2 // Index of entry before the new one
	prevLogTerm := -1
	if prevLogIndex >= 0 {
		prevLogTerm = s.log[prevLogIndex].Term
	}

	return WRITE_OK, &AppendEntriesRequest{
		Term:         s.currentTerm,
		LeaderID:     s.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      []LogEntry{entry}, // Send the new entry
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

	prevLogIndex := len(s.log) - 2 // Index of entry before the new one
	prevLogTerm := -1
	if prevLogIndex >= 0 {
		prevLogTerm = s.log[prevLogIndex].Term
	}

	return CAS_OK, &AppendEntriesRequest{
		Term:         s.currentTerm,
		LeaderID:     s.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      []LogEntry{entry}, // Send the new entry
		LeaderCommit: s.commitIndex,
	}, ""
}

func (l *Leader) WaitForReplication(s *Server, followerID string, success bool, term int) (MessageType, *AppendEntriesRequest, []ConfirmedOperation) {
	// If the server is no longer the leader, return appropriate message
	if s.currentState != LEADER {
		println("I am not the leader WaitForReplication")
		return NOT_LEADER, nil, nil
	}

	// como só o lider espera pela replicacao, os terms tem de ter maioria
	if s.currentTerm != term && !s.vs.isFollowerTermValid(followerID, CurrentTerm(s.currentTerm), term) {
		println("WaitForReplication: term mismatch or not verified, ignoring response from", followerID)
		// Note: Consider if this should lead to stepping down if term is higher,
		// or if it's just an old/invalid response.
		// The block below handles stepping down for higher terms.
		return ERROR, nil, nil
	}

	// If the leader receives a term higher than its current term, step down
	if term > s.currentTerm {
		s.currentTerm = term
		println("WaitForReplication Leader stepping down due to higher term from follower", followerID, "new term:", s.currentTerm)
		s.currentState = FOLLOWER
		s.leaderId = ""
		s.votedFor = ""
		// Reset leader-specific volatile state (nextIndex, matchIndex are part of l *Leader)
		// Clearing them here ensures they are re-initialized if this node becomes leader again.
		l.nextIndex = make(map[string]int)
		l.matchIndex = make(map[string]int)
		return ERROR, nil, nil // Or NOT_LEADER
	}

	// Handle unsuccessful AppendEntries response (log inconsistency)
	if !success {
		println("WaitForReplication: AppendEntries failed for follower", followerID, "decrementing nextIndex")
		// Decrement nextIndex for the follower and retry
		if l.nextIndex[followerID] > 0 { // Ensure nextIndex doesn't go below 0
			l.nextIndex[followerID]--
		} else if l.nextIndex[followerID] == 0 { // If nextIndex is 0, can't decrement further for valid log indices
			// No action or specific handling if already at the start
		}

		// Prepare to send the entry at the new (decremented) nextIndex
		nextIdxToSend := l.nextIndex[followerID]
		prevLogIndexForRetry := nextIdxToSend - 1
		prevLogTermForRetry := -1
		if prevLogIndexForRetry >= 0 && prevLogIndexForRetry < len(s.log) {
			prevLogTermForRetry = s.log[prevLogIndexForRetry].Term
		} else if prevLogIndexForRetry == -1 {
			// prevLogTerm is implicitly -1 or 0 depending on convention for "no entry"
			prevLogTermForRetry = -1 // Explicitly
		}

		entriesToRetry := make([]LogEntry, 0)
		if nextIdxToSend >= 0 && nextIdxToSend < len(s.log) { // Ensure entry exists
			entriesToRetry = append(entriesToRetry, s.log[nextIdxToSend])
		} else {
			// If nextIdxToSend is out of bounds (e.g. < 0 or >= len(s.log)),
			// it implies we might need to send a heartbeat or handle an edge case.
			// For now, if no specific entry, send empty (like a probing heartbeat).
		}

		retryRequest := &AppendEntriesRequest{
			Term:         s.currentTerm,
			LeaderID:     s.id,
			PrevLogIndex: prevLogIndexForRetry,
			PrevLogTerm:  prevLogTermForRetry,
			Entries:      entriesToRetry,
			LeaderCommit: s.commitIndex,
		}
		return RETRY_SEND, retryRequest, nil
	}

	// ----- If success is true -----
	println("WaitForReplication: AppendEntries succeeded for follower", followerID)

	// Correctly update matchIndex and nextIndex.
	// The success implies that the follower accepted the entries in the AppendEntries RPC.
	// We need to know what was the `prevLogIndex` and `len(entries)` of THAT RPC.
	// Since this function doesn't have that directly, we use a common Raft approach:
	// The leader *sent* entries starting from `old_nextIndex = l.nextIndex[followerID]`.
	// Let's assume the successful RPC contained all entries from `old_nextIndex` up to `len(s.log)-1`.
	// This is a simplification. A more robust way would be to know exactly what was acked.
	//
	// A common pattern upon success:
	// matchIndex[followerID] = prevLogIndex_of_the_RPC + number_of_entries_in_the_RPC
	// nextIndex[followerID] = matchIndex[followerID] + 1
	//
	// Given the current structure, when a CAS/Write happens, one entry is sent.
	// Its index is len(s.log)-1 (at the time of sending). prevLogIndex was len(s.log)-2.
	// So, matchIndex should become (len(s.log)-2) + 1 = len(s.log)-1.
	//
	// Heuristic: If nextIndex[followerID] was k, and leader sent log[k], and it's acked:
	//   matchIndex[followerID] = k
	//   nextIndex[followerID] = k + 1
	// If it was a heartbeat success, follower is up to len(s.log)-1:
	//   matchIndex[followerID] = len(s.log)-1
	//   nextIndex[followerID] = len(s.log)

	// Get the value of nextIndex *before* it might have been changed by a failed attempt or this success.
	// This is tricky as this function is reacting to a response.
	// The key is that a success implies the follower has all entries up to a certain point.
	// We assume the success is for entries up to the leader's current log state,
	// relative to what the leader *thought* the follower needed.

	// If nextIndex[followerID] was pointing to an entry that was sent,
	// and that RPC was successful, then matchIndex should be updated to that entry's index.
	if _, ok := l.nextIndex[followerID]; !ok {
		// This should not happen if initialized at leader transition. Log an error or re-initialize.
		println("ERROR: nextIndex for follower", followerID, "not initialized!")
		// Fallback initialization (less ideal here)
		lastLogIdx := -1
		if len(s.log) > 0 {
			lastLogIdx = len(s.log) - 1
		}
		l.nextIndex[followerID] = lastLogIdx + 1
		l.matchIndex[followerID] = -1 // Default to -1
	}

	// If the leader's log is not empty, the follower should at least match up to the end of the leader's log upon success.
	// This means the AppendEntries it just ACKed contained entries that brought it up to some point.
	// The simplest model is that success makes the follower's matchIndex equal to the latest entry in the leader's log,
	// and nextIndex one beyond that. This works if the AppendEntries was for latest entries or a heartbeat.
	if len(s.log) > 0 {
		// The follower has successfully replicated entries.
		// We assume the successful AppendEntries contained entries up to the leader's current log length -1.
		// Or it was a heartbeat confirming the follower is up-to-date.
		newMatchIndex := len(s.log) - 1
		if l.matchIndex[followerID] < newMatchIndex { // Only advance matchIndex
			l.matchIndex[followerID] = newMatchIndex
		}
		// Next index should always be one past the match index.
		l.nextIndex[followerID] = l.matchIndex[followerID] + 1
		println("Updated matchIndex[", followerID, " (success)]=", l.matchIndex[followerID], "nextIndex[", followerID, "]=", l.nextIndex[followerID])

	} else { // Leader's log is empty
		l.matchIndex[followerID] = -1 // No entries to match
		l.nextIndex[followerID] = 0   // Next entry would be at index 0
		println("Updated matchIndex[", followerID, " (success, empty log)]=", l.matchIndex[followerID], "nextIndex[", followerID, "]=", l.nextIndex[followerID])
	}

	// Determine new commit index
	// A log entry is committed once the leader that created the entry has
	// replicated it on a majority of the servers.
	newCommitIndex := s.commitIndex
	// Iterate from commitIndex + 1 up to the last index in the log
	for index := s.commitIndex + 1; index < len(s.log); index++ {
		// Check if the entry is from the current leader's term
		if s.log[index].Term != s.currentTerm {
			// Entries from previous terms cannot be committed using this rule alone.
			// (Raft commits entries from previous terms using a slightly different mechanism involving a current term entry)
			// For simplicity here, we only advance commitIndex for current term entries.
			continue
		}

		count := 1 // Count the leader itself
		for fID, matchIdx := range l.matchIndex {
			if fID != s.id && matchIdx >= index {
				count++
			}
		}

		println("For log index", index, "(term", s.log[index].Term, "), replication count:", count, " (majority:", s.majority, ")")

		if count >= s.majority {
			newCommitIndex = index
		} else {
			// If this entry isn't replicated on a majority, subsequent entries cannot be either.
			break
		}
	}
	println("Old commit index:", s.commitIndex, "Potential new commit index:", newCommitIndex)

	// Apply entries and collect confirmed operations
	var confirmedOps []ConfirmedOperation
	if newCommitIndex > s.commitIndex {
		s.commitIndex = newCommitIndex
		println()
		println()
		println("Advanced commitIndex to:", s.commitIndex)

		// Apply all committed entries that haven't been applied yet
		for i := s.lastApplied + 1; i <= s.commitIndex; i++ {
			// Ensure 'i' is within log bounds, especially if log shrinks or commitIndex is stale.
			if i < 0 || i >= len(s.log) {
				println("Error: trying to apply out-of-bounds log index", i, "len(log) is", len(s.log))
				continue // or break, or handle error
			}

			entry := s.log[i]
			parts := strings.Split(entry.Command, " ")
			var response map[string]interface{}
			var messageType Operation // Assuming Operation is an enum/string type for CAS, WRITE etc.
			//var opSuccess bool = false // To track if the operation itself (like CAS) was successful

			println("Applying to state machine: log[", i, "]=", entry.Command)

			switch parts[0] {
			case "write":
				if len(parts) >= 3 {
					key := parts[1]
					value := parts[2]
					s.stateMachine.kv[key] = value
					messageType = WRITE // Ensure WRITE is defined
					response = map[string]interface{}{
						"type": "write_ok",
					}
					//opSuccess = true
				}
				break

			case "cas":
				println("Committing CAS operation from log:", entry.Command)
				if len(parts) >= 4 {
					key := parts[1]
					from := parts[2]
					to := parts[3]
					currentValue, exists := s.stateMachine.kv[key]

					if !exists {
						messageType = CAS_INVALID_KEY // Ensure these constants are defined
						response = map[string]interface{}{
							"type": "error",
							"code": 20, // Maelstrom error code for key does not exist
							"text": fmt.Sprintf("CAS failed: key '%s' does not exist", key),
						}
					} else if fmt.Sprintf("%v", currentValue) != from { // Convert currentValue to string for comparison
						messageType = CAS_INVALID_FROM
						displayMessage := fmt.Sprintf("CAS failed for key '%s': current value '%v' does not match 'from' value '%s'", key, currentValue, from)
						response = map[string]interface{}{
							"type": "error",
							"code": 22, // Maelstrom error code for precondition failed
							"text": displayMessage,
						}
					} else {
						s.stateMachine.kv[key] = to
						messageType = CAS
						response = map[string]interface{}{
							"type": "cas_ok",
						}
						// opSuccess = true
					}
				}
				break
			}

			// Record the confirmed operation if we have a client message to respond to
			if response != nil && entry.Message != nil {
				println("State machine applied, entry.Message found for response. Type:", response["type"])
				confirmedOp := ConfirmedOperation{
					ClientMessage: entry.Message,
					MessageFrom:   entry.MessageFrom,
					MessageType:   messageType,
					Response:      response,
				}
				confirmedOps = append(confirmedOps, confirmedOp)
			} else if entry.Message != nil {
				println("State machine applied, entry.Message found BUT no response generated for op:", entry.Command)
			}
			s.lastApplied = i
		}
	}

	return WRITE_OK, nil, confirmedOps // WRITE_OK here might just mean "operation processed" not specific to write
}
