package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"
)

type AlertConfirmation struct {
	Confirmations int
	TotalNodes    int
}

// LogEntry represents an entry in the Raft log
type LogEntry struct {
	Term    int    `json:"term"`
	Index   int    `json:"index"`
	Command string `json:"command"`
	// usado para retornar uma resposta a quem a mandou
	Message     *MessageInternal
	MessageFrom string // last node that sent the message to the leader
	Signature   string // signature of the entry
}

func (e *LogEntry) HashEntry() string {
	// Implement a hash function for the entry
	h := sha256.New()
	h.Write([]byte(e.Command))
	h.Write([]byte(string(e.Term)))
	h.Write([]byte(string(e.Index)))
	return hex.EncodeToString(h.Sum(nil))
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
	pendingAlerts       map[string]*AlertConfirmation
}

func (s *Server) Lock() {
	if s == nil {
		return
	}
	if s.mutex == nil {
		s.mutex = &sync.Mutex{}
	}
	// println("\033[31mNode " + s.id + " is locking\033[0m\n")
	s.mutex.Lock()
}

func (s *Server) Unlock() {
	if s == nil {
		return
	}
	// println("\033[32mNode " + s.id + " is unlocking\033[0m\n")
	s.mutex.Unlock()
}

// NewServer initializes a new Raft server
func NewServer(id string, nodes []string,
	leaderHeartbeatFunc func(msg map[string]interface{}),
	candidateStartNewElection func(msg map[string]interface{}),
) *Server {
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
		pendingAlerts:       make(map[string]*AlertConfirmation),
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
				// println("\\033[31mTIMER GOT RESETED, NO LEADER CONTACTED\\033[0m")
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
				// println("\\033[31mLEADER IS SENDING ANOTHER HEARTBEAT\\033[0m")
				msg := s.leader.GetHeartbeatMessage(s, s.id)
				s.resetLeaderTimeout()
				s.Unlock()
				leaderHeartbeatFunc(msg)

				// byzantine behavior simulation
				if s.id == "n0" && !s.leader.attack.alreadyAttacked {
					go func() {
						time.Sleep(2 * time.Second)
						s.Lock()
						// Check if logs exist and modify some entry
						if len(s.log) > 3 {

							// quero alterar a posicao 2
							prevLogTerm := s.log[len(s.log)-3].Term
							prevLogIndex := s.log[len(s.log)-3].Index
							newListOfEntries := s.log[len(s.log)-2:]

							// print new list
							println(" ")
							println("prevLogTerm:", prevLogTerm, "prevLogIndex:", prevLogIndex)
							println(" ")
							println("Byzantine: new list")
							for _, entry := range newListOfEntries {
								println("Index:", entry.Index, "Term:", entry.Term, "Command:", entry.Command)
							}
							s.leader.attack.Attack(s, prevLogTerm, prevLogIndex, newListOfEntries)

							println("\033[31m[BYZANTINE] Node n0 altered log entry at index 0\033[0m")
						}
						s.Unlock()
					}()
				}

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

func (s *Server) ToStringLogs() string {
	var logs []string
	for _, entry := range s.log {
		// also print if they are committed
		if entry.Index <= s.commitIndex {
			logs = append(logs, fmt.Sprintf("Index: %d, Term: %d, Command: %s (committed)", entry.Index, entry.Term, entry.Command))
		} else {
			logs = append(logs, fmt.Sprintf("Index: %d, Term: %d, Command: %s", entry.Index, entry.Term, entry.Command))
		}
	}
	return strings.Join(logs, "\n")
}

func (s *Server) becomeCandidate(followerToCandidateFunc func(msg map[string]interface{})) {
	s.currentState = CANDIDATE
	s.leaderId = ""
	msg := s.candidate.StartElection(s.id)
	msg["majority"] = s.majority
	msg["has"] = len(s.candidate.Votes)
	followerToCandidateFunc(msg)
}

// resetElectionTimeout resets the election timer with a random duration
func (s *Server) resetElectionTimeout() {
	minTimeout := 150
	maxTimeout := 300
	timeout := minTimeout + rand.Intn(maxTimeout-minTimeout)
	// println("RESETTING ELECTION TIMEOUT TO", timeout, "ms")
	s.timer.Reset(time.Millisecond * time.Duration(timeout))
}

func (s *Server) resetLeaderTimeout() {
	value := 100
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
	IsAttack     bool       `json:"attack"` // só para debugging
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
	s.Lock()
	defer s.Unlock()
	msgToReturn := s.follower.AppendEntries(s, msg)

	println(" ")
	println(s.ToStringLogs())
	println(" ")

	if msgToReturn["reset_timeout"] == 1 {
		s.resetElectionTimeout()
	}
	return msgToReturn
}

// WaitForReplication processes follower responses for replication

const (
	CAS                 = "CAS"
	READ                = "READ"
	WRITE               = "WRITE"
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
	s.Lock()
	defer s.Unlock()
	return s.leader.WaitForReplication(s, followerID, success, followerTerm)
}

func (s *Server) Write(key string, value string, originalMessage *MessageInternal, msgFrom string) (MessageType, *AppendEntriesRequest, string) {
	// println("\033[32mNode " + s.id + " is processing a write\033[0m\n")
	s.Lock()
	defer s.Unlock()
	if s.currentState != LEADER {
		return NOT_LEADER, nil, s.leaderId
	}
	return s.leader.Write(s, key, value, originalMessage, msgFrom)
}

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
		return NOT_LEADER, nil, s.leaderId
	}
	return s.leader.Cas(s, key, from, to, originalMessage, msgFrom)
}
