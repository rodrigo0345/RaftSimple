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
	kv map[string]any
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

func (s *Server) Lock() {
	println("\033[31mNode " + s.id + " is locking\033[0m\n")
	s.mutex.Lock()
}

func (s *Server) Unlock() {
	println("\033[32mNode " + s.id + " is unlocking\033[0m\n")
	s.mutex.Unlock()
}

// NewServer initializes a new Raft server
func NewServer(id string, nodes []string,
	leaderHeartbeatFunc func(msg map[string]interface{}),
	candidateStartNewElection func(msg map[string]interface{})) *Server {
	kv := &KeyValueStore{kv: map[string]any{}}
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

	// wait for replication of the votes
	// time.Sleep(time.Millisecond * time.Duration(rand.Intn(150)))

	go func() {
		for {
			<-s.timer.C
			s.Lock()
			switch s.currentState {
			case FOLLOWER:
				println("\\033[31mTIMER GOT RESETED, NO LEADER CONTACTED\\033[0m")
				s.becomeCandidate(candidateStartNewElection)
				s.resetElectionTimeout()
				s.Unlock()
				break
			case CANDIDATE:
				msg := s.candidate.StartElection(s.id)
				candidateStartNewElection(msg)
				s.resetElectionTimeout()
				s.Unlock()
				break
			case LEADER:
				println("\\033[31mLEADER IS SENDING ANOTHER HEARTBEAT\\033[0m")
				msg := s.leader.GetHeartbeatMessage(s, s.id)
				s.resetLeaderTimeout()
				s.Unlock()
				leaderHeartbeatFunc(msg)
				break
			default:
				s.Unlock()
			}

		}
	}()
	if nodeIDs[0] == s.id {
		s.timer.Reset(0)
	}

	return s
}

func (s *Server) becomeCandidate(followerToCandidateFunc func(msg map[string]interface{})) {
	s.currentState = CANDIDATE
	msg := s.candidate.StartElection(s.id)
	msg["majority"] = s.majority
	msg["has"] = len(s.candidate.Votes)
	followerToCandidateFunc(msg)
}

// resetElectionTimeout resets the election timer with a random duration
func (s *Server) resetElectionTimeout() {
	println("RESETING ELECTION TIMEOUT 300ms")
	value := 100000000 // vamos ver se não há bugs no resto por enquanto
	s.timer.Reset(time.Millisecond * time.Duration(value))
}

func (s *Server) resetLeaderTimeout() {
	println("RESETING LEADER TIMEOUT 40ms")
	value := 40
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
	s.Lock()
	defer s.Unlock()
	s.resetElectionTimeout()
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
	println("\033[32mNode " + s.id + " is processing append entry\033[0m\n")
	s.Lock()
	defer s.Unlock()
	msgToReturn := s.follower.AppendEntries(s, msg)
	if msgToReturn["reset_timeout"] == 1 {
		s.resetElectionTimeout()
	}
	return msgToReturn
}

// Read retrieves a value from the key-value store
func (s *Server) Read(key string) (MessageType, any) {
	println("\033[32mNode " + s.id + " is processing a read\033[0m\n")
	s.Lock()
	defer s.Unlock()

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
	println("\033[32mNode " + s.id + " is processing wait for replication\033[0m\n")
	s.Lock()
	defer s.Unlock()
	return s.leader.WaitForReplication(s, followerID, success, followerTerm)
}

func (s *Server) Write(key string, value string) (MessageType, *AppendEntriesRequest, string) {
	println("\033[32mNode " + s.id + " is processing a write\033[0m\n")
	s.Lock()
	defer s.Unlock()
	if s.currentState != LEADER {
		return NOT_LEADER, nil, s.leaderId
	}
	return s.leader.Write(s, key, value)
}

func (s *Server) Cas(key string, from string, to string) MessageType {
	return CAS_OK
}
