
package main

type Leader struct {
  id int
  // for each node there is a nextIndex
  nextIndex map[int]int
  matchIndex map[int]int
  node *Server
}

func NewLeader(nodeInfo *Server) *Leader {
  return &Leader{
    id: nodeInfo.nodes[0],
    nextIndex: map[int]int{},
    matchIndex: map[int]int{},
    node: nodeInfo,
  }
}

func (l *Leader) IsLeader(node *Server) bool {
  return node.id == l.id
}

