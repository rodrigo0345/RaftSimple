package main

type Candidate struct {
  Term int
  Votes map[string]int
  // votes needed for majority
  NeededVotes int
}

func NewCandidate() *Candidate {
  return &Candidate{
    Term: 0,
    Votes: map[string]int{},
  }
}

func (c *Candidate) StartElection(follower *Follower) {
  c.Term = follower.node.currentTerm + 1
  c.Votes = map[string]int{}
  c.Votes[follower.id] = 1
  c.NeededVotes = len(follower.node.nodes)/2 + 1
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


