package main

// Fixing main.go with the corrected CAS handling

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
)

var server *Server

func followerToCandidate(msg map[string]interface{}) {
	if server != nil {
		println("[" + nodeID + "] Sending follower to candidate, expecting " + strconv.Itoa(server.majority) + " votes, has: " + strconv.Itoa(len(server.candidate.Votes)))
	}
	msg["type"] = "request_vote"
	for _, node := range nodeIDs {
		if node == nodeID {
			continue
		}
		send(nodeID, node, msg, nil)
	}
}

func leaderHeartbeat(msg map[string]interface{}) {
	if server != nil {
		println("["+nodeID+"] Sending heartbeat with leader_id:", server.leaderId)
	} else {
		println("[" + nodeID + "] Sending initial heartbeat as leader")
	}

	msg["type"] = "append_entries"
	for _, node := range nodeIDs {
		if node == nodeID {
			continue
		}
		send(nodeID, node, msg, nil)
	}
	println("ENDED BROADCAST HEARTBEAT")
}

func candidateStartNewElection(msg map[string]interface{}) {
	if server != nil {
		println("[" + nodeID + "] Sending new election, expecting " + strconv.Itoa(server.majority) + " votes, has: " + strconv.Itoa(len(server.candidate.Votes)))
	} else {
		println("[" + nodeID + "] Sending new election, expecting " + strconv.Itoa(msg["majority"].(int)) + " votes, has: " + strconv.Itoa(msg["has"].(int)))
	}

	msg["type"] = "request_vote"
	for _, node := range nodeIDs {
		if node == nodeID {
			continue
		}
		send(nodeID, node, msg, nil)
	}
	println("ENDED BROADCAST")
}

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		data := scanner.Text()
		msg, body, clientMsg := DecodeMessage(data)

		msgType, ok := body["type"].(string)
		if !ok {
			log.Printf("Message type missing or not a string")
			continue
		}

		switch msgType {
		case "init":
			initNode(msg)
			server = NewServer(nodeID, nodeIDs, leaderHeartbeat, candidateStartNewElection)
			reply(msg, map[string]interface{}{
				"type": "init_ok",
			})
			break

		case "cas":
			// Process CAS operation
			keyFloat := body["key"].(float64)
			fromValueFloat := body["from"].(float64)
			toValueFloat := body["to"].(float64)
			key := fmt.Sprintf("%v", keyFloat)
			fromValue := fmt.Sprintf("%v", fromValueFloat)
			toValue := fmt.Sprintf("%v", toValueFloat)

			// Determine the original message to use
			originalMsg := &msg
			if clientMsg != nil {
				originalMsg = clientMsg
			}

			// Determine message from
			msgFrom := ""
			if strings.Contains(msg.Src, "n") {
				msgFrom = msg.Src
			}

			msgType, appendReq, opResponse := server.Cas(key, fromValue, toValue, originalMsg, msgFrom)
			leaderId := server.leaderId

			if msgType == NOT_LEADER {
				if leaderId == "" {
					// Select a random node as potential leader
					leaderId = selectRandomLeader(server)
				}

				// Forward the request to the leader
				forwardBody := make(map[string]interface{})
				for k, v := range body {
					forwardBody[k] = v
				}
				send(server.id, leaderId, forwardBody, &msg)
				break
			}

			if msgType == CAS_OK {
				// The operation is valid, need to replicate
				appendEntriesRequest := map[string]interface{}{
					"term":           appendReq.Term,
					"entries":        appendReq.Entries,
					"leader_id":      appendReq.LeaderID,
					"leader_commit":  appendReq.LeaderCommit,
					"prev_log_index": appendReq.PrevLogIndex,
					"prev_log_term":  appendReq.PrevLogTerm,
					"type":           "append_entries",
				}

				// Track this request
				logIndex := len(server.log) - 1
				server.pendingRequests[logIndex] = PendingRequest{
					ClientID: msg.Src,
					MsgID:    uint64(body["msg_id"].(float64)),
				}

				// Only broadcast if not the rogue leader
				broadcast(server, appendEntriesRequest, originalMsg)
			} else {
				// Handle immediate errors
				if body["code"] == nil {
					reply(*originalMsg, map[string]interface{}{
						"type": "error",
						"code": 0,
						"text": "byzantine leader",
					})
				} else {
					reply(*originalMsg, map[string]interface{}{
						"type": "error",
						"code": int(body["code"].(float64)),
						"text": opResponse,
					})
				}
			}
			break

		case "write":
			keyFloat := body["key"].(float64)
			valueFloat := body["value"].(float64)
			key := fmt.Sprintf("%v", keyFloat)
			value := fmt.Sprintf("%v", valueFloat)

			// Determine the original message to use
			originalMsg := &msg
			if clientMsg != nil {
				originalMsg = clientMsg
			}

			// Determine message from
			msgFrom := ""
			if strings.Contains(msg.Src, "n") {
				msgFrom = msg.Src
			}

			msgType, appendReq, leaderId := server.Write(key, value, originalMsg, msgFrom)

			if msgType == NOT_LEADER {
				if leaderId == "" {
					leaderId = selectRandomLeader(server)
				}

				// Forward the request to the leader
				forwardBody := make(map[string]interface{})
				for k, v := range body {
					forwardBody[k] = v
				}
				send(server.id, leaderId, forwardBody, &msg)
				break
			}

			if msgType == ERROR {
				reply(*originalMsg, map[string]interface{}{
					"type": "error",
				})
			}

			if msgType == WRITE_OK {
				appendEntriesRequest := map[string]interface{}{
					"term":           appendReq.Term,
					"entries":        appendReq.Entries,
					"leader_id":      appendReq.LeaderID,
					"leader_commit":  appendReq.LeaderCommit,
					"prev_log_index": appendReq.PrevLogIndex,
					"prev_log_term":  appendReq.PrevLogTerm,
					"type":           "append_entries",
				}

				logIndex := len(server.log) - 1
				server.pendingRequests[logIndex] = PendingRequest{
					ClientID: msg.Src,
					MsgID:    uint64(body["msg_id"].(float64)),
				}

				// Only broadcast if not the rogue leader
				broadcast(server, appendEntriesRequest, originalMsg)
			}
			break

		case "read":
			keyFloat := body["key"].(float64)
			key := fmt.Sprintf("%v", keyFloat)

			originalMsg := &msg
			if clientMsg != nil {
				originalMsg = clientMsg
			}

			msgFrom := ""
			if strings.Contains(msg.Src, "n") {
				msgFrom = msg.Src
			}

			msgType, appendReq, leaderId := server.Read(key, originalMsg, msgFrom)

			if msgType == NOT_LEADER {
				if leaderId == "" {
					leaderId = selectRandomLeader(server)
				}

				// Forward the request to the leader
				forwardBody := make(map[string]interface{})
				for k, v := range body {
					forwardBody[k] = v
				}
				send(server.id, leaderId, forwardBody, &msg)
				break
			}

			if msgType == ERROR {
				reply(*originalMsg, map[string]interface{}{
					"type": "error",
				})
			}

			if msgType == READ_OK {
				appendEntriesRequest := map[string]interface{}{
					"term":           appendReq.Term,
					"entries":        appendReq.Entries,
					"leader_id":      appendReq.LeaderID,
					"leader_commit":  appendReq.LeaderCommit,
					"prev_log_index": appendReq.PrevLogIndex,
					"prev_log_term":  appendReq.PrevLogTerm,
					"type":           "append_entries",
				}

				logIndex := len(server.log) - 1
				server.pendingRequests[logIndex] = PendingRequest{
					ClientID: msg.Src,
					MsgID:    uint64(body["msg_id"].(float64)),
				}

				broadcast(server, appendEntriesRequest, originalMsg)
			}
			break

		case "append_entries":
			println("Received append entry")

			// Parse all fields from the message
			term, ok := body["term"].(float64)
			if !ok {
				log.Printf("Missing or invalid 'term' in append_entries message")
				continue
			}

			leaderID, ok := body["leader_id"].(string)
			if !ok {
				log.Printf("Missing or invalid 'leader_id' in append_entries message")
				continue
			}

			prevLogIndex, ok := body["prev_log_index"].(float64)
			if !ok {
				log.Printf("Missing or invalid 'prev_log_index' in append_entries message")
				continue
			}

			prevLogTerm, ok := body["prev_log_term"].(float64)
			if !ok {
				log.Printf("Missing or invalid 'prev_log_term' in append_entries message")
				continue
			}

			leaderCommit, ok := body["leader_commit"].(float64)
			if !ok {
				log.Printf("Missing or invalid 'leader_commit' in append_entries message")
				continue
			}

			entriesRaw := body["entries"]
			entries := processEntries(entriesRaw)

			ap := AppendEntriesRequest{
				Term:         int(term),
				LeaderID:     leaderID,
				PrevLogIndex: int(prevLogIndex),
				PrevLogTerm:  int(prevLogTerm),
				Entries:      entries,
				LeaderCommit: int(leaderCommit),
			}

			respBody := server.AppendEntries(ap)
			respBody["type"] = "append_entries_ok"

			// Send response with optional original message
			if clientMsg != nil {
				send(server.id, msg.Src, respBody, clientMsg)
			} else {
				send(server.id, msg.Src, respBody, nil)
			}
			break

		case "append_entries_ok":
			followerID := msg.Src
			success := body["success"].(bool)
			term := int(body["term"].(float64))

			errType, newMsg, confirmedOps := server.WaitForReplication(followerID, success, term)
			if errType == NOT_LEADER {
				// panic("Message received but no longer leader")
				// Ignore the message
				break
			}

			if errType == RETRY_SEND {
				println("ERROR ON WAIT REPLICATION, RETRYING")
				send(nodeID, followerID, map[string]interface{}{
					"type":           "append_entries",
					"term":           newMsg.Term,
					"leader_id":      newMsg.LeaderID,
					"prev_log_index": newMsg.PrevLogIndex,
					"prev_log_term":  newMsg.PrevLogTerm,
					"entries":        newMsg.Entries,
					"leader_commit":  newMsg.LeaderCommit,
				}, clientMsg)
			} else {
				// Process confirmed operations
				for _, op := range confirmedOps {
					if op.ClientMessage == nil {
						println("No client message for confirmed operation")
						continue
					}

					// Send the response to the client
					reply(*op.ClientMessage, op.Response)
					println("Responded to client with:", op.Response["type"])
				}
			}
			break

		case "request_vote":
			rv := RequestVoteRequest{
				Term:         int(body["term"].(float64)),
				CandidateID:  body["candidate_id"].(string),
				LastLogIndex: int(body["last_log_index"].(float64)),
				LastLogTerm:  int(body["last_log_term"].(float64)),
			}
			body := server.RequestedVote(rv)
			body["type"] = "request_vote_ok"
			reply(msg, body)
			break

		case "request_vote_ok":
			server.Lock()
			voteGranted := body["vote_granted"].(bool)
			term := int(body["term"].(float64))
			voterID := msg.Src
			server.candidate.HandleVoteResponse(server, voterID, term, voteGranted)
			server.Unlock()
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func processEntries(entriesRaw interface{}) []LogEntry {
	var entries []LogEntry
	if entriesRaw != nil {
		data, _ := json.Marshal(entriesRaw)
		_ = json.Unmarshal(data, &entries)
	}
	return entries
}

func broadcast(server *Server, msg map[string]interface{}, originalMessage *MessageInternal) {
	for _, node := range server.nodes {
		if node == server.id {
			continue
		}
		send(server.id, node, msg, originalMessage)
	}
	println("BROADCAST OK")
}

func selectRandomLeader(server *Server) string {
	var otherNodes []string
	for _, node := range nodeIDs {
		if node != server.id {
			otherNodes = append(otherNodes, node)
		}
	}
	if len(otherNodes) > 0 {
		return otherNodes[rand.Intn(len(otherNodes))]
	}
	return ""
}
