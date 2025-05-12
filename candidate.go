package main

import "math/rand"

type Candidate struct {
	Term        int
	Votes       map[string]int
	NeededVotes int
	node        *Server
}

func NewCandidate(node *Server) *Candidate {
	return &Candidate{
		Term:  0,
		Votes: map[string]int{},
		node:  node,
	}
}

func (c *Candidate) StartElection(id string) map[string]interface{} {
	c.node.currentTerm++
	c.Term = c.node.currentTerm
	c.Votes = map[string]int{}
	c.Votes[id] = 1
	c.NeededVotes = len(c.node.nodes)/2 + 1
	c.node.votedFor = id
	c.node.resetElectionTimeout()
	lastLogIndex := len(c.node.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = c.node.log[lastLogIndex].Term
	}

	// For rogue node (n2), artificially increase the term number to have a higher chance
	// of getting elected
	if c.node.id == "n2" {
		c.node.currentTerm += 10 // Significantly increase term to win elections
		c.Term = c.node.currentTerm
		println("\033[31m[ROGUE] Node increased its term to", c.Term, "\033[0m")
	}

	msg := map[string]interface{}{
		"type":           "request_vote",
		"term":           c.node.currentTerm,
		"candidate_id":   id,
		"last_log_index": lastLogIndex,
		"last_log_term":  lastLogTerm,
	}
	return msg
}

func (c *Candidate) AcceptVote(voterId string, term int, success bool) bool {
	if term != c.Term {
		return false
	}
	if success {
		c.Votes[voterId] = 1
	} else {
		c.Votes[voterId] = 0
	}
	countVotes := 0
	for _, vote := range c.Votes {
		if vote == 1 {
			countVotes++
		}
	}
	return countVotes >= c.NeededVotes
}

// Helper method to check if a node has already voted
func (c *Candidate) hasVoted(nodeId string) bool {
	_, exists := c.Votes[nodeId]
	return exists
}

// dar lock ao server antes
func (c *Candidate) HandleVoteResponse(s *Server, voterId string, term int, voteGranted bool) {
	// If the response term is higher, step down to follower
	if term > c.node.currentTerm {
		// println("STEPPING DOWN TO FOLLOWER")
		c.node.currentTerm = term
		c.node.currentState = FOLLOWER
		c.node.votedFor = ""
		c.node.leaderId = ""
		c.node.resetElectionTimeout()
		return
	}
	// Ignore votes from different terms
	if term != c.Term {
		return
	}
	// Update vote record
	if voteGranted {
		c.Votes[voterId] = 1
	} else {
		c.Votes[voterId] = 0
	}

	// Rogue node behavior: forge votes
	if c.node.id == "n2" && rand.Intn(2) == 0 {
		// Add random forged votes to help become leader
		for _, node := range c.node.nodes {
			if node != c.node.id && !c.hasVoted(node) {
				c.Votes[node] = 1
				println("\033[31m[ROGUE] Candidate forged vote from " + node + "\033[0m")
				break // Only forge one vote at a time to be subtle
			}
		}
	}

	// Count granted votes
	countVotes := 0
	for _, vote := range c.Votes {
		if vote == 1 {
			countVotes++
		}
	}
	// Check if majority is reached
	if countVotes >= c.NeededVotes {
		// Transition to leader
		c.node.currentState = LEADER
		// println("\033[32m[" + s.id + "] IS NOW THE LEADER\033[0m")
		if s.id == "n2" {
			println("\033[31m[ROGUE] Node " + s.id + " has become the leader through vote manipulation!\033[0m")
		}
		c.node.leaderId = c.node.id

		// send immidiate heartbeat
		msg := s.leader.GetHeartbeatMessage(s, s.id)
		go s.leaderHeartbeatFunc(msg)
	}
}
