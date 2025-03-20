package main

type Follower struct {
	id string
	// for each node there is a nextIndex
	nextIndex  map[int]int
	matchIndex map[int]int
	node       *Server
  ellectionTimeout int
}

func NewFollower(nodeInfo *Server) *Follower {
	return &Follower{
		id:         nodeInfo.id,
		nextIndex:  map[int]int{},
		matchIndex: map[int]int{},
		node:       nodeInfo,
	}
}

func (f *Follower) AppendEntries(leaderTerm int, leaderId int, prevLogIndex int, prevLogTerm int, leaderCommit int, entries []*LogEntry) (int, bool) {
    // Update term if leader's term is newer
    if leaderTerm > f.node.currentTerm {
        f.node.currentTerm = leaderTerm
    }
    // Reject if leader's term is outdated
    if leaderTerm < f.node.currentTerm {
        return f.node.currentTerm, false
    }
    // Check log consistency at prevLogIndex
    if prevLogIndex > 0 && (prevLogIndex > len(f.node.log) || f.node.log[prevLogIndex-1].term != prevLogTerm) {
        return f.node.currentTerm, false
    }
    // Truncate log after prevLogIndex
    if prevLogIndex < len(f.node.log) {
        f.node.log = f.node.log[:prevLogIndex]
    }
    // Append new entries
    f.node.log = append(f.node.log, entries...)
    // Update commit index
    if leaderCommit > f.node.commitIndex {
        var lastIndex int
        if len(f.node.log) > 0 {
            lastIndex = f.node.log[len(f.node.log)-1].index
        } else {
            lastIndex = 0
        }
        if leaderCommit < lastIndex {
            f.node.commitIndex = leaderCommit
        } else {
            f.node.commitIndex = lastIndex
        }
    }
    // Success
    return f.node.currentTerm, true
}

func (f *Follower) RequestVote(candidateTerm int, candidateId int, lastLogIndex int, lastLogTerm int) (int, bool) {
    // Reject if candidate's term is outdated
    if candidateTerm < f.node.currentTerm {
        return f.node.currentTerm, false
    }
    // Update term if candidate's term is newer
    if candidateTerm > f.node.currentTerm {
        f.node.currentTerm = candidateTerm
        f.node.votedFor = 0
    }
    // Check if already voted for another candidate in this term
    if f.node.votedFor != 0 && f.node.votedFor != candidateId {
        return f.node.currentTerm, false
    }
    // Determine follower's last log term and index
    var followerLastTerm, followerLastIndex int
    if len(f.node.log) > 0 {
        followerLastTerm = f.node.log[len(f.node.log)-1].term
        followerLastIndex = f.node.log[len(f.node.log)-1].index
    } else {
        followerLastTerm = 0
        followerLastIndex = 0
    }
    // Check if candidate's log is at least as up-to-date
    if lastLogTerm < followerLastTerm || (lastLogTerm == followerLastTerm && lastLogIndex < followerLastIndex) {
        return f.node.currentTerm, false
    }
    // Grant vote
    f.node.votedFor = candidateId
    return f.node.currentTerm, true
}
