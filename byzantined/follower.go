package main

import (
	"log"
	"strings"
)

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
	log.Printf("[Follower %s] Received AppendEntries: term=%d, prevLogIndex=%d, prevLogTerm=%d, entries=%d\n", s.id, msg.Term, msg.PrevLogIndex, msg.PrevLogTerm, len(msg.Entries))
	response := make(map[string]interface{})
	response["reset_timeout"] = 0
	response["term"] = s.currentTerm

	// message can be ignored that is from the past
	if msg.Term < s.currentTerm {
		log.Printf("[Follower %s] Reject AppendEntries: term %d < currentTerm %d\n", s.id, msg.Term, s.currentTerm)
		response["term"] = s.currentTerm
		response["success"] = false
		return response
	}

	if s.currentState == LEADER && msg.Term == s.currentTerm {
		log.Printf("[Follower %s] Reject AppendEntries: I'm leader with term %d\n", s.id, s.currentTerm)
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

		log.Printf("[Follower %s] Reject AppendEntries: log inconsistency at prevLogIndex %d\n", s.id, msg.PrevLogIndex)
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
	log.Printf("[Follower %s] Appended %d entries, new log length: %d\n", s.id, len(msg.Entries), len(s.log))

	// podemos dar commit a mais mensagens, caso o lider tenha dado commit a mensagens que o follower não deu
	if msg.LeaderCommit > s.commitIndex {
		lastNewEntryIndex := msg.PrevLogIndex + len(msg.Entries)
		if msg.LeaderCommit < lastNewEntryIndex {
			s.commitIndex = msg.LeaderCommit
		} else {
			s.commitIndex = lastNewEntryIndex
		}
		log.Printf("[Follower %s] Updated commitIndex to %d\n", s.id, s.commitIndex)

		// aplica tudo que tenha sido commit à máquina de estados do follower, não aplica read
		for i := s.lastApplied + 1; i <= s.commitIndex; i++ {
			entry := s.log[i]
			log.Printf("[Follower %s] Applying entry index %d command %s\n", s.id, entry.Index, entry.Command)
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
