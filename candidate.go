package main

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

func (c *Candidate) HandleVoteResponse(s *Server, voterId string, term int, voteGranted bool) {
	// If the response term is higher, step down to follower
	if term > c.node.currentTerm {
		c.node.currentTerm = term
		c.node.currentState = FOLLOWER
		c.node.votedFor = ""
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
		c.node.leaderId = c.node.id
		// Stop the election timer
		c.node.timer.Stop()
		// Send an immediate heartbeat to assert leadership
		go func() {
			heartbeatMsg := c.node.leader.GetHeartbeatMessage(s, c.node.id)
			leaderHeartbeat(heartbeatMsg)
		}()
	}
}
