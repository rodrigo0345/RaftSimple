package main

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// LogEntry represents an entry in the Raft log
type LogEntry struct {
	Term    int    `json:"term"`
	Index   int    `json:"index"`
	Command string `json:"command"`
	// usado para retornar uma resposta a quem a mandou
	Message     *MessageInternal
	MessageFrom string // last node that sent the message to the leader
}

// KeyValueStore is the state machine for the Raft system
type KeyValueStore struct {
	kv map[string]string
}

func (s *KeyValueStore) ToString() string {
	var keys []string
	for k := range s.kv {
		keys = append(keys, k)
	}

	// Sort keys for consistent output (optional but improves readability)
	sort.Strings(keys)

	var builder strings.Builder
	for i, key := range keys {
		if i > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(fmt.Sprintf("%s=%v", key, s.kv[key]))
	}
	return builder.String()
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
	id                  string
	nodes               []string
	commitIndex         int
	lastApplied         int
	currentTerm         int
	votedFor            string
	majority            int
	log                 []LogEntry
	currentState        State
	leader              *Leader
	candidate           *Candidate
	follower            *Follower
	stateMachine        *KeyValueStore
	leaderId            string
	timer               *time.Timer
	mutex               *sync.Mutex
	pendingRequests     map[int]PendingRequest // Added to track client requests
	leaderHeartbeatFunc func(msg map[string]interface{})

	vs *ValidationStore
}

func (s *Server) Lock() {
	if s.mutex == nil {
		s.mutex = &sync.Mutex{}
	}
	// println("\033[31mNode " + s.id + " is locking\033[0m\n")
	s.mutex.Lock()
}

func (s *Server) Unlock() {
	// println("\033[32mNode " + s.id + " is unlocking\033[0m\n")
	s.mutex.Unlock()
}

// NewServer initializes a new Raft server
func NewServer(id string, nodes []string,
	leaderHeartbeatFunc func(msg map[string]interface{}),
	candidateStartNewElection func(msg map[string]interface{})) *Server {
	kv := &KeyValueStore{kv: map[string]string{}}
	leader := NewLeader()
	follower := NewFollower()
	s := &Server{
		id:                  id,
		nodes:               nodes,
		commitIndex:         -1,
		lastApplied:         -1,
		currentTerm:         0,
		votedFor:            "",
		majority:            len(nodes)/2 + 1,
		log:                 []LogEntry{},
		currentState:        FOLLOWER,
		leader:              leader,
		follower:            follower,
		stateMachine:        kv,
		leaderId:            "",
		timer:               time.NewTimer(time.Millisecond * time.Duration(rand.Intn(150)+150)),
		mutex:               &sync.Mutex{},
		pendingRequests:     make(map[int]PendingRequest),
		leaderHeartbeatFunc: leaderHeartbeatFunc,

		// anti-byzantine
		vs: newValidationStore(),
	}
	s.candidate = NewCandidate(s) // Pass server instance to Candidate

	// wait for replication of the votes
	// time.Sleep(time.Millisecond * time.Duration(rand.Intn(150)))

	go func() {
		for {
			<-s.timer.C
			fmt.Fprintf(os.Stderr, "\033[33mNode %s: Timer expired, current state: %d\033[0m\n", s.id, s.currentState)
			s.Lock()
			switch s.currentState {
			case FOLLOWER:
				// println("\\033[31mTIMER GOT RESETED, NO LEADER CONTACTED\\033[0m")
				msg := s.candidate.StartElection(s, s.id, true)
				candidateStartNewElection(msg)
				// s.becomeCandidate(candidateStartNewElection)
				s.resetElectionTimeout()
				s.Unlock()
				break
			case CANDIDATE:
				msg := s.candidate.StartElection(s, s.id, true)
				candidateStartNewElection(msg)
				s.resetElectionTimeout()
				s.Unlock()
				break
			case LEADER:
				// println("\\033[31mLEADER IS SENDING ANOTHER HEARTBEAT\\033[0m")
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

/////////////////////////////////
// solução anti-term forgery
/////////////////////////////////

func (s *Server) validateTerm(term int, validatorId string, isValid bool, implicitSend bool) bool {
	s.Lock()
	defer s.Unlock()

	// se neste ponto já tem maioria, é sinal que já enviou o request_vote
	// esta variavel é para evitar que o leader envie duplicados
	stop := s.vs.hasMajorityAccepted(s, CurrentTerm(s.currentTerm+1))

	// First time seeing this term
	s.vs.addValidationProposer(validatorId, CurrentTerm(s.currentTerm+1), term, isValid)

	if !stop && s.vs.hasMajorityAccepted(s, CurrentTerm(s.currentTerm+1)) {
		// Launch election broadcast
		if implicitSend {
			// only set currentTerm when a majority is reached
			s.currentTerm = term
			s.votedFor = ""
			msg := s.candidate.StartElection(s, s.id, false)
			followerToCandidateFunc := candidateStartNewElection // reuse callback
			msg["type"] = "request_vote"
			followerToCandidateFunc(msg)
		}
		return true
	}

	if stop {
		return false
	}

	receivedMajorityTerm := s.vs.hasMajorityRefused(s, CurrentTerm(s.currentTerm+1))
	if receivedMajorityTerm > -1 {
		println("\033[31m[ROGUE] Node", s.id, "received majority of refusals for term", term, "\033[0m")
		s.currentTerm = receivedMajorityTerm
		s.votedFor = ""
		s.currentState = FOLLOWER
		s.leaderId = ""
		s.resetElectionTimeout()
		return true
	}

	return false
}

func (s *Server) requestTermValidation(term int) map[string]interface{} {
	// vote on self
	s.vs.voteForSelf(s.id, CurrentTerm(term))

	return map[string]interface{}{
		"type":    "validate_term",
		"term":    term,
		"node_id": s.id,
	}
}

func (s *Server) processTermValidation(term int, nodeId string) map[string]interface{} {
	s.Lock()
	defer s.Unlock()

	isValid := s.vs.addValidationFollower(nodeId, CurrentTerm(s.currentTerm), term)
	return map[string]interface{}{
		"type":      "validate_term_response",
		"term":      term,
		"validator": s.id,
		"is_valid":  isValid,
	}
}

/////////////////////////////////
// solução anti-term forgery
/////////////////////////////////

func (s *Server) becomeCandidate(followerToCandidateFunc func(msg map[string]interface{})) {
	s.currentState = CANDIDATE
	s.leaderId = ""
	s.currentTerm += 1

	// nesta versão alterada, envia apenas um request_validate para validar o termo
	msg := s.candidate.StartElection(s, s.id, true)
	msg["majority"] = s.majority
	msg["has"] = len(s.candidate.Votes)
	followerToCandidateFunc(msg)
}

// resetElectionTimeout resets the election timer with a random duration
func (s *Server) resetElectionTimeout() {
	minTimeout := 150
	maxTimeout := 300
	if s.id == "n2" { // Rogue node
		minTimeout = 50
		maxTimeout = 100
	}
	timeout := minTimeout + rand.Intn(maxTimeout-minTimeout)
	// println("RESETTING ELECTION TIMEOUT TO", timeout, "ms")
	s.timer.Reset(time.Millisecond * time.Duration(timeout))
}

func (s *Server) resetLeaderTimeout() {
	value := 20
	// println("RESETTING LEADER TIMEOUT TO", value, "ms")
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
	// println("\033[32mNode " + s.id + " is processing append entry\033[0m\n")
	s.PrintLogs()

	s.Lock()
	defer s.Unlock()

	msgToReturn := s.follower.AppendEntries(s, msg)
	if msgToReturn["reset_timeout"] == 1 {
		s.resetElectionTimeout()
	}
	return msgToReturn
}

func (s *Server) PrintLogs() {
	s.Lock()
	defer s.Unlock()

	println("")
	println("[State machine]")

	// print if it is commited or not
	for i, entry := range s.log {
		if i <= s.commitIndex {
			println("[COMMITTED] " + entry.Command)
		} else {
			println("[NOT COMMITTED] " + entry.Command)
		}
	}
}

// WaitForReplication processes follower responses for replication

const (
	CAS                 = "CAS"
	WRITE               = "WRITE"
	READ                = "READ"
	CAS_INVALID_KEY     = "INVALID_KEY"
	CAS_INVALID_FROM    = "INVALID_FROM"
	INVALID_REPLICATION = "INVALID_REPLICATION"
)

type Operation string
type ConfirmedOperation struct {
	ClientMessage *MessageInternal       `json:"client_message"`
	MessageFrom   string                 `json:"message_from"`
	MessageType   Operation              `json:"message_type"`
	Response      map[string]interface{} `json:"response"`
}

func (s *Server) WaitForReplication(followerID string, success bool, followerTerm int) (MessageType, *AppendEntriesRequest, []ConfirmedOperation) {
	// println("\033[32mNode " + s.id + " is processing wait for replication\033[0m\n")
	// s.PrintLogs()

	s.Lock()
	defer s.Unlock()
	return s.leader.WaitForReplication(s, followerID, success, followerTerm)
}

func (s *Server) Write(key string, value string, originalMessage *MessageInternal, msgFrom string) (MessageType, *AppendEntriesRequest, string) {
	// println("\033[32mNode " + s.id + " is processing a write\033[0m\n")
	s.Lock()
	defer s.Unlock()
	if s.currentState != LEADER {
		println("NOT LEADER")
		return NOT_LEADER, nil, s.leaderId
	}
	println("Processing write")
	return s.leader.Write(s, key, value, originalMessage, msgFrom)
}

// Read retrieves a value from the key-value store
func (s *Server) Read(key string, originalMessage *MessageInternal, msgFrom string) (MessageType, *AppendEntriesRequest, string) {
	// println("\033[32mNode " + s.id + " is processing a read\033[0m\n")
	s.Lock()
	defer s.Unlock()
	if s.currentState != LEADER {
		return NOT_LEADER, nil, s.leaderId
	}
	return s.leader.Read(s, key, originalMessage, msgFrom)
}

func (s *Server) Cas(key string, from string, to string, originalMessage *MessageInternal, msgFrom string) (MessageType, *AppendEntriesRequest, string) {
	// println("\033[32mNode " + s.id + " is processing a CAS\033[0m\n")
	s.Lock()
	defer s.Unlock()
	if s.currentState != LEADER {
		println("NOT LEADER")
		return NOT_LEADER, nil, s.leaderId
	}
	println("Processing CAS")
	return s.leader.Cas(s, key, from, to, originalMessage, msgFrom)
}
