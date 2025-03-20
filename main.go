package main

import (
	"bufio"
	"encoding/json"
	"log"
	"os"
  "time"
)

func followerToCandidate(msg *Message) {
}

func leaderHeartbeat(msg *Message) {
}

func candidateStartNewElection(msg *Message) {
}

func main() {
  var server *Server

	for scanner.Scan() {
		data := scanner.Text()

		var msg Message
		if err := json.Unmarshal([]byte(data), &msg); err != nil {
			log.Fatal(err)
		}

		switch msg.Body.Type {
		case "init":
			initNode(msg)

			server = NewServer(nodeID, nodeIDs, followerToCandidate, leaderHeartbeat, candidateStartNewElection)
			reply(msg, map[string]interface{}{
				"type": "init_ok",
			})
			break
		case "append_entries":

      body := server.AppendEntries(msg)
			reply(msg, body)

      break
    case "append_entries_ok":
      
      break
		case "request_vote":

      body := server.RequestedVote(msg)
      reply(msg, body)

      break
    case "request_vote_ok":
      if currentState != "candidate" {
        break
      }
      isLeader := candidate.AcceptVote(msg.Body["node_id"].(int), msg.Body["term"].(int), msg.Body["success"].(bool))
      if isLeader {
        currentState = "leader"
      }
    break
      
		}

		reply(msg, map[string]interface{}{
			"msg": "Hello, world!",
		})
	}
}
