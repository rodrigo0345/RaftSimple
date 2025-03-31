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

func (l *Leader) Write(s *Server, key string, value string) (MessageType, *AppendEntriesRequest, string) {
	entry := LogEntry{
		Term:    s.currentTerm,
		Index:   len(s.log),
		Command: "write " + key + " " + value,
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

func (l *Leader) WaitForReplication(s *Server, followerID string, success bool, term int) (MessageType, *AppendEntriesRequest, []ConfirmedWrite) {

	// se ele acha que é lider mas recebe uma mensagem com term maior, deixa de ser lider
	if term > s.currentTerm {
		s.currentTerm = term
		s.currentState = FOLLOWER
		s.leaderId = ""
		s.votedFor = ""
		return ERROR, nil, nil
	}

	if s.currentState != LEADER {
		return NOT_LEADER, nil, nil
	}

	// se o follower se queixou que o log e as entries enviadas não davam match
	if !success {

		// tenta decrementar 1
		if s.leader.nextIndex[followerID] > 1 {
			s.leader.nextIndex[followerID]--
		}
		nextIdx := s.leader.nextIndex[followerID]
		prevLogIndex := nextIdx - 1

		var prevLogTerm int = -1
		if prevLogIndex >= 0 {
			prevLogTerm = s.log[prevLogIndex].Term
		}

		var entries []LogEntry
		if nextIdx < len(s.log) {
			// não posso mandar logo a lista toda desde nextIdx até ao fim?
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

	// acho que isto está mal, tem aqui edge cases que não estão a ser tratados
	// lastLogIndex := len(s.leader.nextIndex[followerID] - 1) ou algo do estilo (???)
	lastLogIndex := len(s.log) - 1
	s.leader.matchIndex[followerID] = lastLogIndex
	s.leader.nextIndex[followerID] = lastLogIndex + 1

	// TODO: lembrar como isto funciona
	for index := s.commitIndex + 1; index <= lastLogIndex; index++ {
		if s.log[index].Term != s.currentTerm {
			continue
		}

		// count the number of nodes that already accepted the message
		count := 1
		for fid, matchIdx := range s.leader.matchIndex {
			if fid != s.id && matchIdx >= index {
				count++
			}
		}

		// enquanto não houver uma maioria não vale a pena fazer nada
		if count < s.majority {
			break
		}

		// ao ter maioria, podemos aplicar na state machine o log
		s.commitIndex = index
		confirmedEntries := make([]ConfirmedWrite, 0)
		for i := s.lastApplied + 1; i <= s.commitIndex; i++ {
			parts := strings.Split(s.log[i].Command, " ")
			if parts[0] == "write" && len(parts) == 3 {
				s.stateMachine.kv[parts[1]] = parts[2]
			}
			s.lastApplied = i
			entryConfirmed := ConfirmedWrite{
				Value: parts[2],
			}
			confirmedEntries = append(confirmedEntries, entryConfirmed)
		}
		return WRITE_OK, nil, confirmedEntries
	}
	return WRITE_OK, nil, nil
}
