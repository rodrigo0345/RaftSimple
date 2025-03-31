package main

import (
	"math/rand"
	"sync"
	"time"
)

// LogEntry represents an entry in the Raft log
type LogEntry struct {
	Term    int    `json:"term"`
	Index   int    `json:"index"`
	Command string `json:"command"`
	// usado para retornar uma resposta a quem a mandou
	Message map[string]interface{}
}

// KeyValueStore is the state machine for the Raft system
type KeyValueStore struct {
	kv map[string]string
}

// State constants for Raft roles
const (
	CANDIDATE = iota
	LEADER
	FOLLOWER
)

type State int

// MessageType constants for client responses
const (
	READ_OK    = "read_ok"
	WRITE_OK   = "write_ok"
	CAS_OK     = "cas_ok"
	ERROR      = "error"
	NOT_LEADER = "not_leader"
	RETRY_SEND = "retry_send"
)

type MessageType string

// PendingRequest tracks client requests awaiting commit
type PendingRequest struct {
	ClientID string
	MsgID    uint64
}

// Server represents a Raft node
type Server struct {
	id              string
	nodes           []string
	commitIndex     int
	lastApplied     int
	currentTerm     int
	votedFor        string
	majority        int
	log             []LogEntry
	currentState    State
	leader          *Leader
	candidate       *Candidate
	follower        *Follower
	stateMachine    *KeyValueStore
	leaderId        string
	timer           *time.Timer
	mutex           *sync.Mutex
	pendingRequests map[int]PendingRequest // Added to track client requests
}

// NewServer initializes a new Raft server
func NewServer(id string, nodes []string, followerToCandidateFunc func(msg map[string]interface{}),
	leaderHeartbeatFunc func(msg map[string]interface{}),
	candidateStartNewElection func(msg map[string]interface{})) *Server {
	kv := &KeyValueStore{kv: map[string]string{}}
	leader := NewLeader()
	follower := NewFollower()
	s := &Server{
		id:              id,
		nodes:           nodes,
		commitIndex:     0,
		lastApplied:     0,
		currentTerm:     0,
		votedFor:        "",
		majority:        len(nodes)/2 + 1,
		log:             []LogEntry{},
		currentState:    FOLLOWER,
		leader:          leader,
		follower:        follower,
		stateMachine:    kv,
		leaderId:        "",
		timer:           time.NewTimer(time.Millisecond * time.Duration(rand.Intn(150)+150)),
		mutex:           &sync.Mutex{},
		pendingRequests: make(map[int]PendingRequest),
	}
	s.candidate = NewCandidate(s) // Pass server instance to Candidate

	if nodeIDs[0] == s.id {
		s.becomeCandidate(leaderHeartbeatFunc)
	}

	// wait for replication of the votes
	// time.Sleep(time.Millisecond * time.Duration(rand.Intn(150)))

	go func() {
		for {
			<-s.timer.C
			s.mutex.Lock()
			switch s.currentState {
			case FOLLOWER:
				s.becomeCandidate(leaderHeartbeatFunc)
			case CANDIDATE:
				msg := s.candidate.StartElection(s.id)
				candidateStartNewElection(msg)
			case LEADER:
				msg := s.leader.GetHeartbeatMessage(s, s.id)
				leaderHeartbeatFunc(msg)
				s.resetLeaderTimeout()
				return
			}
			s.resetElectionTimeout()
			s.mutex.Unlock()
		}
	}()
	return s
}

func (s *Server) becomeCandidate(followerToCandidateFunc func(msg map[string]interface{})) {
	s.currentState = CANDIDATE
	s.votedFor = s.id
	s.currentTerm++
	lastLogIndex := len(s.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = s.log[lastLogIndex].Term
	}
	msg := s.follower.BecomeCandidate(s.id, s.currentTerm, lastLogTerm, lastLogIndex)
	followerToCandidateFunc(msg)
}

// resetElectionTimeout resets the election timer with a random duration
func (s *Server) resetElectionTimeout() {
	value := rand.Intn(151) + 150
	s.timer.Reset(time.Millisecond * time.Duration(value))
}

func (s *Server) resetLeaderTimeout() {
	value := rand.Intn(30) + 30
	s.timer.Reset(time.Millisecond * time.Duration(value))
}

type RequestVoteRequest struct {
	Term         int    `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex int    `json:"last_log_index"`
	LastLogTerm  int    `json:"last_log_term"`
}

// RequestedVote handles a vote request from a candidate
func (s *Server) RequestedVote(msg RequestVoteRequest) map[string]interface{} {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.follower.Vote(s, msg)
}

// AppendEntriesRequest is the structure for append entries requests
type AppendEntriesRequest struct {
	Term         int        `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex int        `json:"prev_log_index"`
	PrevLogTerm  int        `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int        `json:"leader_commit"`
}

// AppendEntries handles log replication from the leader
func (s *Server) AppendEntries(msg AppendEntriesRequest) map[string]interface{} {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.follower.AppendEntries(s, msg)
}

// Read retrieves a value from the key-value store
func (s *Server) Read(key string) (MessageType, string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.currentState != LEADER {
		return NOT_LEADER, key
	}

	// acho que vou ter de mudar isto para ser apenas o lider a fazer,
	// caso contrario, acho que não é linearizable
	return READ_OK, s.stateMachine.kv[key]
}

// WaitForReplication processes follower responses for replication
type ConfirmedWrite struct {
	Value string `json:"value"`
}

func (s *Server) WaitForReplication(followerID string, success bool, followerTerm int) (MessageType, *AppendEntriesRequest, []ConfirmedWrite) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.leader.WaitForReplication(s, followerID, success, followerTerm)
}

func (s *Server) Write(key string, value string) (MessageType, *AppendEntriesRequest, string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.currentState != LEADER {
		return NOT_LEADER, nil, s.leaderId
	}
	return s.leader.Write(s, key, value)
}

func (s *Server) Cas(key string, from string, to string) MessageType {
	return CAS_OK
}
