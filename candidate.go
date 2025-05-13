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

func (c *Candidate) StartElection(id string, isRequestingValidation bool) map[string]interface{} {

	if isRequestingValidation {
		c.node.currentTerm++
	}
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

	if !isRequestingValidation {
		return msg
	}

	// TODO: tentar voltar a colocar isto normal
	return c.node.requestTermValidation(c.node.currentTerm)
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
	// 1) Se o termo da resposta for maior, passo a Follower
	if term > c.node.currentTerm {
		c.node.currentTerm = term
		c.node.currentState = FOLLOWER
		c.node.votedFor = ""
		c.node.leaderId = ""
		c.node.resetElectionTimeout()
		return
	}
	// 2) Se for de termo diferente do meu, ignoro
	if term != c.Term {
		return
	}

	// 3) Verifico se este voter participou do quorum de validação do termo
	validations, ok := s.termValidations[term]
	println("Term:", term)
	println("Validations:", validations)

	if !ok || !validations[voterId] {
		// Não validou o term => voto possivelmente forjado, ignoro
		println("\033[33m[DEFENSE] Ignoring vote from", voterId, "- term not validated\033[0m")
		return
	}

	// 4) Registo o voto válido
	if voteGranted {
		c.Votes[voterId] = 1
	} else {
		c.Votes[voterId] = 0
	}

	// 5) Conto apenas votos de quem validou o termo
	grantedCount := 0
	for voter, v := range c.Votes {
		if v == 1 && validations[voter] {
			grantedCount++
		}
	}

	// 6) Se atingir quorum de votos (NeedVotes), torno-me líder
	if grantedCount >= c.NeededVotes {
		c.node.currentState = LEADER
		c.node.leaderId = c.node.id
		s.leaderId = c.node.id
		println("\033[32m[" + s.id + "] IS NOW THE LEADER\033[0m")
		// Envio batimento imediato
		msg := s.leader.GetHeartbeatMessage(s, s.id)
		go s.leaderHeartbeatFunc(msg)
	}
}
