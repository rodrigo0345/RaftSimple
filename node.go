package main

import (
	"math/rand"
	"sync"
	"time"
)

type LogEntry struct {
	term    int
	index   int
	command string
}

type KeyValueStore struct {
	kv map[string]string
}

const (
	CANDIDATE = iota
	LEADER
	FOLLOWER
)

type State int

type Server struct {
	id    string
	nodes []string

	commitIndex int
	lastApplied int
	currentTerm int
	votedFor    *int // nil if not voted for
	majority    int

	log          []*LogEntry
	currentState State
	leader       *Leader
	candidate    *Candidate
	follower     *Follower
	stateMachine *KeyValueStore
	mutex        *sync.Mutex

	timer *time.Timer
}

func NewServer(id string, nodes []string, followerToCandidateFunc func(msg *Message),
	leaderHeartbeatFunc func(msg *Message),
	candidateStartNewElection func(msg *Message)) *Server {
	kv := &KeyValueStore{kv: map[string]string{}}
	leader := NewLeader()
	candidate := NewCandidate()
	follower := NewFollower()

	rand.Seed(time.Now().UnixNano())
	timer := time.NewTimer(time.Millisecond * time.Duration(rand.Intn(150)+150))

	majority := len(nodes)/2 + 1

	s := &Server{
		id:    id,
		nodes: nodes,

		commitIndex: 0,
		lastApplied: 0,
		currentTerm: 0,
		votedFor:    nil,
		majority:    majority,

		log: []*LogEntry{},

		currentState: FOLLOWER,

		leader:       leader,
		candidate:    candidate,
		follower:     follower,
		stateMachine: kv,

		timer: timer,
		mutex: &sync.Mutex{},
	}

	go func() {
		<-s.timer.C

		s.mutex.Lock()
		defer s.mutex.Unlock()

		switch s.currentState {
		case FOLLOWER:
			s.currentState = CANDIDATE
			msg := s.follower.BecomeCandidate()
			followerToCandidateFunc(msg)
			break
		case CANDIDATE:
			msg := s.candidate.StartElection()
			candidateStartNewElection(msg)
			break
		case LEADER:
			msg := s.leader.GetHeartbeatMessage()
			leaderHeartbeatFunc(msg)
			break
		}

		value := rand.Intn(151) + 150
		s.timer.Reset(time.Millisecond * time.Duration(value))
	}()

	return s
}

const (
	READ_OK  = "read_ok"
	WRITE_OK = "write_ok"
	CAS_OK   = "cas_ok"
	ERROR    = "error"
)

type MessageType string

///////////////////////////////////
// interface para os outros nós
///////////////////////////////////

// candidate requested vote
func (s *Server) RequestedVote() map[string]interface{} {
}

func (s *Server) AppendEntries() map[string]interface{} {
	// se for lider, ignorar
	// reset ao timer do follower
}

///////////////////////////////////
// interface com cliente
///////////////////////////////////

func (s *Server) Read(key string) (MessageType, string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return READ_OK, s.stateMachine.kv[key]
}

func (s *Server) Write(key string, value string) MessageType {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	// enviar mensagem ao lider a dizer que quero escrever
	s.stateMachine.kv[key] = value
	return WRITE_OK
}

func (s *Server) Cas(key string, from string, to string) MessageType {
	// demasiado complexo, o stor disse para não fazer
	return CAS_OK
}
