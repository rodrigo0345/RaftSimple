package main

import (
	"encoding/base64"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"
)

// LogEntry represents an entry in the Raft log
type LogEntry struct {
	Term        int      `json:"term"`
	Index       int      `json:"index"`
	Command     string   `json:"command"`
	Cumulative  [32]byte `json:"cumulative"`
	Message     *MessageInternal
	MessageFrom string
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

// LogDigest represents the digest of a node's log
type LogDigest struct {
	LastIndex int      `json:"last_index"`
	Hash      [32]byte `json:"hash"`
}

// Server represents a Raft node
type Server struct {
	id                  string
	byzantineMode       bool
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
	pendingRequests     map[int]PendingRequest
	leaderHeartbeatFunc func(msg map[string]interface{})
	// Digest exchange state
	digestTimer     *time.Timer
	currentNeighbor int      // Index for round-robin neighbor selection
	neighborNodes   []string // Sorted list of other nodes
}

func (s *Server) Lock() {
	if s.mutex == nil {
		s.mutex = &sync.Mutex{}
	}
	s.mutex.Lock()
}

func (s *Server) Unlock() {
	s.mutex.Unlock()
}

// NewServer initializes a new Raft server
func NewServer(id string, nodes []string,
	leaderHeartbeatFunc func(msg map[string]interface{}),
	candidateStartNewElection func(msg map[string]interface{})) *Server {
	kv := &KeyValueStore{kv: map[string]string{}}
	leader := NewLeader()
	follower := NewFollower()
	neighborNodes := make([]string, 0, len(nodes)-1)
	for _, node := range nodes {
		if node != id {
			neighborNodes = append(neighborNodes, node)
		}
	}
	sort.Strings(neighborNodes)
	s := &Server{
		id:                  id,
		byzantineMode:       byzantineMode,
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
		digestTimer:         time.NewTimer(time.Millisecond * 500),
		currentNeighbor:     0,
		neighborNodes:       neighborNodes,
	}
	s.candidate = NewCandidate(s)

	go func() {
		for {
			select {
			case <-s.timer.C:
				s.Lock()
				switch s.currentState {
				case FOLLOWER:
					s.becomeCandidate(candidateStartNewElection)
					s.resetElectionTimeout()
					s.Unlock()
				case CANDIDATE:
					msg := s.candidate.StartElection(s.id)
					candidateStartNewElection(msg)
					s.resetElectionTimeout()
					s.Unlock()
				case LEADER:
					msg := s.leader.GetHeartbeatMessage(s, s.id)
					s.resetLeaderTimeout()
					s.Unlock()
					leaderHeartbeatFunc(msg)
				default:
					s.Unlock()
				}
			case <-s.digestTimer.C:
				s.Lock()
				s.sendLogDigest()
				s.resetDigestTimer()
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
	s.leaderId = ""
	msg := s.candidate.StartElection(s.id)
	msg["majority"] = s.majority
	msg["has"] = len(s.candidate.Votes)
	followerToCandidateFunc(msg)
}

func (s *Server) resetElectionTimeout() {
	minTimeout := 150
	maxTimeout := 300
	timeout := minTimeout + rand.Intn(maxTimeout-minTimeout)
	s.timer.Reset(time.Millisecond * time.Duration(timeout))
}

func (s *Server) resetLeaderTimeout() {
	value := 100
	s.timer.Reset(time.Millisecond * time.Duration(value))
}

func (s *Server) resetDigestTimer() {
	s.digestTimer.Reset(time.Millisecond * 100)
}

// sendLogDigest sends the current log digest to the next neighbor
func (s *Server) sendLogDigest() {
	// Rebuild neighborNodes to exclude the leader and self
	neighborNodes := make([]string, 0, len(s.nodes)-1)
	for _, node := range s.nodes {
		if node != s.id && node != s.leaderId {
			neighborNodes = append(neighborNodes, node)
		}
	}
	if len(neighborNodes) == 0 {
		// If no neighbors (e.g., only node or all are leader/self), skip
		return
	}
	// Update neighborNodes and reset currentNeighbor if needed
	if len(neighborNodes) != len(s.neighborNodes) {
		s.neighborNodes = neighborNodes
		s.currentNeighbor = 0
	} else {
		// Ensure currentNeighbor is valid
		if s.currentNeighbor >= len(s.neighborNodes) {
			s.currentNeighbor = 0
		}
	}
	neighbor := s.neighborNodes[s.currentNeighbor]
	s.currentNeighbor = (s.currentNeighbor + 1) % len(s.neighborNodes)
	lastIndex := len(s.log) - 1
	var hash [32]byte
	if lastIndex >= 0 {
		hash = s.log[lastIndex].Cumulative
	}
	msg := map[string]interface{}{
		"type":       "log_digest",
		"last_index": lastIndex,
		"hash":       hash[:],
		"term":       s.currentTerm,
	}
	log.Printf("[%s] Sending log_digest to %s: index=%d, hash=%x", s.id, neighbor, lastIndex, hash[:8])
	send(s.id, neighbor, msg, nil)
}

// handleLogDigest processes incoming log_digest messages
func (s *Server) HandleLogDigest(msg map[string]interface{}, src string) {
	term, _ := msg["term"].(float64)
	if int(term) < s.currentTerm {
		return
	}
	lastIndex, _ := msg["last_index"].(float64)
	hashStr, ok := msg["hash"].(string)
	if !ok {
		log.Printf("[%s] Invalid hash format in log_digest from %s", s.id, src)
		return
	}
	hashBytes, err := base64.StdEncoding.DecodeString(hashStr)
	if err != nil || len(hashBytes) != 32 {
		log.Printf("[%s] Failed to decode hash from %s: %v", s.id, src, err)
		return
	}
	var hash [32]byte
	copy(hash[:], hashBytes)
	ownLastIndex := len(s.log) - 1
	var ownHash [32]byte
	if ownLastIndex >= 0 {
		ownHash = s.log[ownLastIndex].Cumulative
	}
	log.Printf("Received digest request index=%d, ownHash=%x, theirHash=%x", ownLastIndex, ownHash[:8], hash[:8])
	if int(lastIndex) == ownLastIndex && hash != ownHash {
		log.Printf("[%s] Detected log divergence with: index=%d, ownHash=%x, theirHash=%x",
			s.id, ownLastIndex, ownHash[:8], hash[:8])
		s.currentState = CANDIDATE
		s.leaderId = ""
		s.votedFor = ""
		s.timer.Reset(0)
	}
}

type RequestVoteRequest struct {
	Term         int    `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex int    `json:"last_log_index"`
	LastLogTerm  int    `json:"last_log_term"`
}

func (s *Server) RequestedVote(msg RequestVoteRequest) map[string]interface{} {
	s.Lock()
	defer s.Unlock()
	s.resetElectionTimeout()
	return s.follower.Vote(s, msg)
}

type AppendEntriesRequest struct {
	Term         int        `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex int        `json:"prev_log_index"`
	PrevLogTerm  int        `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int        `json:"leader_commit"`
}

func (s *Server) AppendEntries(msg AppendEntriesRequest) map[string]interface{} {
	s.Lock()
	defer s.Unlock()
	msgToReturn := s.follower.AppendEntries(s, msg)
	if msgToReturn["reset_timeout"] == 1 {
		s.resetElectionTimeout()
	}
	return msgToReturn
}

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
	s.Lock()
	defer s.Unlock()
	return s.leader.WaitForReplication(s, followerID, success, followerTerm)
}

func (s *Server) Write(key string, value string, originalMessage *MessageInternal, msgFrom string) (MessageType, *AppendEntriesRequest, string) {
	s.Lock()
	defer s.Unlock()
	if s.currentState != LEADER {
		return NOT_LEADER, nil, s.leaderId
	}
	return s.leader.Write(s, key, value, originalMessage, msgFrom)
}

func (s *Server) Read(key string, originalMessage *MessageInternal, msgFrom string) (MessageType, *AppendEntriesRequest, string) {
	s.Lock()
	defer s.Unlock()
	if s.currentState != LEADER {
		return NOT_LEADER, nil, s.leaderId
	}
	return s.leader.Read(s, key, originalMessage, msgFrom)
}

func (s *Server) Cas(key string, from string, to string, originalMessage *MessageInternal, msgFrom string) (MessageType, *AppendEntriesRequest, string) {
	s.Lock()
	defer s.Unlock()
	if s.currentState != LEADER {
		return NOT_LEADER, nil, s.leaderId
	}
	return s.leader.Cas(s, key, from, to, originalMessage, msgFrom)
}
