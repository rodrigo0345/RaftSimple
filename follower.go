package main

import "strings"

type Follower struct {
	nextIndex  map[int]int
	matchIndex map[int]int
}

func NewFollower() *Follower {
	return &Follower{
		nextIndex:  map[int]int{},
		matchIndex: map[int]int{},
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

	if !s.vs.isProposerTermValid(msg.LeaderID, CurrentTerm(s.currentTerm-1), msg.Term) {
		response["term"] = s.currentTerm
		response["success"] = false
		println("\033[33m[DEFENSE] Node", s.id, "rejected suspicious AppendEntries term", msg.Term, "from", msg.LeaderID, "\033[0m")
		return response
	}

	// message can be ignored that is from the past
	if msg.Term < s.currentTerm {
		println("\033[33m[DEFENSE] Node", s.id, "rejected AppendEntries term", msg.Term, "from", msg.LeaderID, "\033[0m")
		response["term"] = s.currentTerm
		response["success"] = false
		return response
	}

	if s.currentState == LEADER {
		response["term"] = s.currentTerm
		response["success"] = false
		println("\033[33m[DEFENSE] Node", s.id, "rejected suspicious AppendEntries term", msg.Term, "from", msg.LeaderID, "\033[0m")
		return response
	}

	if s.currentState == LEADER && msg.Term == s.currentTerm {
		println("\033[33m[DEFENSE] Node", s.id, "rejected AppendEntries term", msg.Term, "from", msg.LeaderID, "\033[0m")
		response["term"] = s.currentTerm
		response["success"] = false
		return response
	}

	s.currentTerm = msg.Term
	s.votedFor = ""
	s.currentState = FOLLOWER
	s.leaderId = msg.LeaderID
	s.resetElectionTimeout()

	// era so um ping
	if len(msg.Entries) == 0 {
		response["success"] = true
		response["term"] = s.currentTerm
		response["reset_timeout"] = 1
		return response
	}

	// as entries não encaixam com o log deste follower
	if msg.PrevLogIndex >= len(s.log) ||
		(msg.PrevLogIndex >= 0 && s.log[msg.PrevLogIndex].Term != msg.PrevLogTerm) {

		response["term"] = s.currentTerm
		response["success"] = false
		return response
	}

	// neste caso, está tudo perfeito e podemos fazer append das entries ao log
	index := msg.PrevLogIndex + 1

	// temos de perder mensagens caso o log tenha demasiadas entradas que o lider não viu
	if index < len(s.log) {
		s.log = s.log[:index]
	}
	s.log = append(s.log, msg.Entries...)

	for _, entry := range msg.Entries {
		if entry.Index < len(s.log) {
			existingEntry := s.log[entry.Index]
			if existingEntry.Term == entry.Term && existingEntry.HashEntry() != entry.HashEntry() {
				alert, _ := NewEquivocationAlertIfApplicable(entry, msg.LeaderID, s)
				if alert != nil {
					// Broadcast the alert to all nodes
					alertMsg := alert.ToRPCMessage()
					broadcast(s, alertMsg, nil)
				}
			}
		}
	}

	// podemos dar commit a mais mensagens, caso o lider tenha dado commit a mensagens que o follower não deu
	if msg.LeaderCommit > s.commitIndex {
		lastNewEntryIndex := msg.PrevLogIndex + len(msg.Entries)
		if msg.LeaderCommit < lastNewEntryIndex {
			s.commitIndex = msg.LeaderCommit
		} else {
			s.commitIndex = lastNewEntryIndex
		}

		// aplica tudo que tenha sido commit à máquina de estados do follower
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
	}
	response["success"] = true

	return response
}

func (f *Follower) Vote(s *Server, msg RequestVoteRequest) map[string]interface{} {

	response := make(map[string]interface{})
	if msg.Term < s.currentTerm {
		response["term"] = s.currentTerm
		response["vote_granted"] = false
		return response
	}

	// suspeita de alteração indevida de termos
	if !s.vs.isProposerTermValid(msg.CandidateID, CurrentTerm(s.currentTerm), msg.Term) {
		response["term"] = s.currentTerm
		response["vote_granted"] = false
		println("\033[33m[DEFENSE] Node", s.id, "rejected suspicious vote term", msg.Term, "from", msg.CandidateID, "\033[0m")
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
