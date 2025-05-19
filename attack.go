package main

import "sort"

// this file is a helper to be able to the
// leader to change commited entries
type Attack struct {
	prevLogTerm  int // valid
	prevLogIndex int // valid

	previousListOfEntries []LogEntry // list of entries before the attack

	// from the changed value onwards
	newListOfEntries []LogEntry
	isDisabled       bool
	alreadyAttacked  bool
}

func NewAttack() *Attack {
	return &Attack{
		prevLogTerm:           -1,
		prevLogIndex:          -1,
		previousListOfEntries: []LogEntry{},
		newListOfEntries:      []LogEntry{},
		isDisabled:            true,
		alreadyAttacked:       false,
	}
}

// set prevLogTerm and prevLogIndex to the last entry before the
// changed entry
func (a *Attack) Attack(s *Server, prevLogTerm, prevLogIndex int, newListOfEntries []LogEntry) {
	if a.alreadyAttacked {
		return
	}
	a.prevLogTerm = prevLogTerm
	a.prevLogIndex = prevLogIndex
	a.previousListOfEntries = s.log
	a.newListOfEntries = newListOfEntries
	a.isDisabled = false
	a.alreadyAttacked = true

	s.log[prevLogIndex+1].Command = "write 0 999"
	s.log = append(s.log[:prevLogIndex+1], newListOfEntries...)

	// use quick sort to sort the log
	sort.Slice(s.log, func(i, j int) bool {
		return s.log[i].Index < s.log[j].Index
	})

	println(" ")
	println("Attack applied")
	println(s.ToStringLogs())
	println(" ")

}

func (a *Attack) GenerateAppendEntriesRequest(s *Server, originalMsg AppendEntriesRequest) AppendEntriesRequest {
	if a.isDisabled {
		originalMsg.IsAttack = false
		return originalMsg
	}

	// disable after the attack
	a.isDisabled = true
	return AppendEntriesRequest{
		IsAttack:     true,
		Term:         s.currentTerm,
		LeaderID:     s.id,
		PrevLogIndex: a.prevLogIndex,
		PrevLogTerm:  a.prevLogTerm,
		LeaderCommit: s.commitIndex,
		Entries:      a.newListOfEntries,
	}
}
