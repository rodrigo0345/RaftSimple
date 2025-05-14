package main

type Candidate struct {
	Term int

	// current term -> voterId -> vote
	Votes       map[int]map[string]int
	NeededVotes int
	node        *Server
}

func NewCandidate(node *Server) *Candidate {
	votes := make(map[int]map[string]int)
	return &Candidate{
		Term:  0,
		Votes: votes,
		node:  node,
	}
}

func (c *Candidate) StartElection(s *Server, id string, isRequestingValidation bool) map[string]interface{} {

	// still cannot do currentTerm + 1, only when a quorum accepted it
	c.Votes[s.currentTerm+1] = map[string]int{}
	c.Votes[s.currentTerm+1][id] = 1
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
		s.currentTerm += 10 // Significantly increase term to win elections
		c.Term = s.currentTerm
		println("\033[31m[ROGUE] Node increased its term to", c.Term, "\033[0m")
	}

	msg := map[string]interface{}{
		"type":           "request_vote",
		"term":           s.currentTerm,
		"candidate_id":   id,
		"last_log_index": lastLogIndex,
		"last_log_term":  lastLogTerm,
	}

	if !isRequestingValidation {
		return msg
	}

	// TODO: tentar voltar a colocar isto normal
	return c.node.requestTermValidation(s.currentTerm + 1)
}

func (c *Candidate) AcceptVote(voterId string, term int, success bool) bool {
	if term != c.Term {
		return false
	}
	if success {
		c.Votes[c.node.currentTerm][voterId] = 1
	} else {
		c.Votes[c.node.currentTerm][voterId] = 0
	}
	countVotes := 0
	for _, vote := range c.Votes[c.node.currentTerm] {
		if vote == 1 {
			countVotes++
		}
	}
	return countVotes >= c.NeededVotes
}

// Helper method to check if a node has already voted
func (c *Candidate) hasVoted(nodeId string) bool {
	_, exists := c.Votes[c.node.currentTerm][nodeId]
	return exists
}

// dar lock ao server antes
func (c *Candidate) HandleVoteResponse(s *Server, voterId string, term int, voteGranted bool) {

	stop := c.alreadyHasMajorityVotes()
	if stop {
		println("\033[33m[DEFENSE] Already has majority votes, ignoring vote from", voterId, "\033[0m")
		return
	}

	// 1) Se o termo da resposta for maior, passo a Follower
	if term > c.node.currentTerm {
		println("\033[33m[DEFENSE] Node", c.node.id, "is now a follower\033[0m")
		c.node.currentTerm = term
		c.node.currentState = FOLLOWER
		c.node.votedFor = ""
		c.node.leaderId = ""
		c.node.resetElectionTimeout()
		return
	}
	// 2) Se for de termo diferente do meu, ignoro
	if term != c.node.currentTerm {
		println("\033[33m[DEFENSE] Ignoring vote from", voterId, "- term mismatch", term, "!= current term", c.Term, "\033[0m")
		return
	}

	// 3) Verifico se este voter participou do quorum de validação do termo
	ok := s.vs.isFollowerTermValid(voterId, CurrentTerm(s.currentTerm), term)
	if !ok {
		// Não validou o term => voto possivelmente forjado, ignoro
		println("\033[33m[DEFENSE] Ignoring vote from", voterId, "- term not validated\033[0m")
		return
	}

	// 4) Registo o voto válido
	if voteGranted {
		c.Votes[c.node.currentTerm][voterId] = 1
	} else {
		c.Votes[c.node.currentTerm][voterId] = 0
	}

	// 5) Conto apenas votos de quem validou o termo
	grantedCount := 0
	for v, _ := range c.Votes[c.node.currentTerm] {
		println("HandleVoteResponse: Voter", v, "voted", v)
		if c.Votes[c.node.currentTerm][v] == 1 {
			grantedCount++
		}
	}

	// 6) Se atingir quorum de votos (NeedVotes), torno-me líder
	if grantedCount >= s.majority {
		c.node.currentState = LEADER
		c.node.leaderId = c.node.id
		s.leaderId = c.node.id
		println("\033[32m[" + s.id + "] IS NOW THE LEADER\033[0m")
		// Envio batimento imediato
		msg := s.leader.GetHeartbeatMessage(s, s.id)
		go s.leaderHeartbeatFunc(msg)
	}
}

func (c *Candidate) alreadyHasMajorityVotes() bool {
	countVotes := 0
	for _, vote := range c.Votes[c.node.currentTerm] {
		if vote == 1 {
			countVotes++
		}
	}
	return countVotes >= c.NeededVotes
}
