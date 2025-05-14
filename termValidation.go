package main

type Validation struct {
	nodeId        string
	proposedTerm  int
	isAccepted    bool
	otherNodeTerm int
}
type CurrentTerm int

type ValidationStore struct {
	termValidations map[CurrentTerm][]Validation
}

func newValidationStore() *ValidationStore {
	vs := &ValidationStore{
		termValidations: make(map[CurrentTerm][]Validation),
	}
	return vs
}

func (vs *ValidationStore) addValidationFollower(nodeId string, currentTerm CurrentTerm, proposedTerm int) bool {
	if _, exists := vs.termValidations[currentTerm]; !exists {
		vs.termValidations[currentTerm] = []Validation{}
	}

	vs.termValidations[currentTerm] = append(vs.termValidations[currentTerm], Validation{
		nodeId:       nodeId,
		proposedTerm: proposedTerm,
		isAccepted:   proposedTerm == int(currentTerm)+1,
	})
	println("Follower", nodeId, "proposed term", proposedTerm, "for current term", currentTerm, "accepted:", proposedTerm == int(currentTerm)+1)
	return proposedTerm == int(currentTerm)+1
}

func (vs *ValidationStore) addValidationProposer(nodeId string, currentTerm CurrentTerm, otherNodeTerm int, isAccepted bool) {
	if _, exists := vs.termValidations[currentTerm]; !exists {
		vs.termValidations[currentTerm] = []Validation{}
	}

	vs.termValidations[currentTerm] = append(vs.termValidations[currentTerm], Validation{
		nodeId:        nodeId,
		proposedTerm:  int(currentTerm),
		isAccepted:    isAccepted,
		otherNodeTerm: otherNodeTerm,
	})
}

func (vs *ValidationStore) hasMajorityAccepted(s *Server, currentTerm CurrentTerm) bool {
	if _, exists := vs.termValidations[currentTerm]; !exists {
		return false
	}

	votes := 0
	for _, v := range vs.termValidations[currentTerm] {
		if v.isAccepted {
			votes++
		}
	}
	isAccepted := votes >= s.majority
	println("Term", currentTerm, "has", votes, "votes, majority is", s.majority, "accepted:", isAccepted)
	return isAccepted
}

// returns the term of the majority of refusals
func (vs *ValidationStore) hasMajorityRefused(s *Server, currentTerm CurrentTerm) int {
	if _, exists := vs.termValidations[currentTerm]; !exists {
		return -1
	}

	temp := make(map[int]int)

	for _, v := range vs.termValidations[currentTerm] {
		temp[v.otherNodeTerm]++
	}

	termAtMajority := -1
	for _, v := range temp {
		if v > s.majority {
			println("Term", v, "has majority refusals")
			termAtMajority = v
		}
	}

	return termAtMajority
}

func (vs *ValidationStore) isFollowerTermValid(nodeId string, currentTerm CurrentTerm, sentTerm int) bool {
	if _, exists := vs.termValidations[currentTerm]; !exists {
		return false
	}

	for _, v := range vs.termValidations[currentTerm] {
		if v.nodeId == nodeId && v.proposedTerm == sentTerm {
			println("[IS VALID] Checking validation for node", v.nodeId, "in term", currentTerm, "has", v.proposedTerm)
			return v.isAccepted
		}
	}
	return false
}

func (vs *ValidationStore) isProposerTermValid(nodeId string, currentTerm CurrentTerm, sentTerm int) bool {
	if _, exists := vs.termValidations[currentTerm]; !exists {
		println("Term", currentTerm, "not found in validations")
		return false
	}

	for _, v := range vs.termValidations[currentTerm] {
		if v.nodeId == nodeId && v.proposedTerm == sentTerm {
			println("[VALID] Found validation for node", v.nodeId, "in term", currentTerm, "has", v.proposedTerm)
			return v.isAccepted
		}
	}
	return false
}

func (vs *ValidationStore) voteForSelf(nodeId string, currentTerm CurrentTerm) {
	if _, exists := vs.termValidations[currentTerm]; !exists {
		vs.termValidations[currentTerm] = []Validation{}
	}

	vs.termValidations[currentTerm] = append(vs.termValidations[currentTerm], Validation{
		nodeId:       nodeId,
		proposedTerm: int(currentTerm),
		isAccepted:   true,
	})
}

func (vs *ValidationStore) getValidationsFollower(nodeId string, currentTerm CurrentTerm) []Validation {
	if _, exists := vs.termValidations[currentTerm]; !exists {
		vs.termValidations[currentTerm] = []Validation{}
	}
	return vs.termValidations[currentTerm]
}

func (vs *ValidationStore) getValidations(currentTerm CurrentTerm) []Validation {
	return vs.termValidations[currentTerm]
}
