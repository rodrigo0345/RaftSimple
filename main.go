package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
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
		msg, body, originalMsg := DecodeMessage(data)

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
			// return error
			reply(msg, map[string]interface{}{
				"type": "error",
			})

		case "write":
			keyFloat := body["key"].(float64)
			valueFloat := body["value"].(float64)
			key := fmt.Sprintf("%v", keyFloat)
			value := fmt.Sprintf("%v", valueFloat)
			msgType, appendReq, leaderId := server.Write(key, value)

			// redirect to the leader
			if msgType == NOT_LEADER {
				if leaderId == "" {
					// Create a list excluding server.id
					var otherNodes []string
					for _, node := range nodeIDs {
						if node != server.id {
							otherNodes = append(otherNodes, node)
						}
					}
					if len(otherNodes) > 0 {
						// Select a random node from otherNodes
						randomNode := otherNodes[rand.Intn(len(otherNodes))]
						leaderId = randomNode
					} else {
						// No other nodes available; handle this case (e.g., log an error)
						fmt.Println("No other nodes available to forward the message")
						return
					}
				}
				println("sending with original message")
				send(server.id, leaderId, body, &msg)
			}

			// if error might mean something that I did not account for
			if msgType == ERROR {
				var msg2 MessageInternal = msg
				if originalMsg != nil {
					msg2 = *originalMsg
				}

				reply(msg2, map[string]interface{}{
					"type": "error",
				})
			}

			// em caso de write ok, temos de fazer append entries aos followers e só
			// depois de termos uma maioria é que podemos responder ao request original
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

				message := originalMsg
				if message == nil {
					message = &msg
				}
				broadcast(server, appendEntriesRequest, message)
			}
			break

		case "read":
			keyFloat := body["key"].(float64)
			key := fmt.Sprintf("%v", keyFloat)
			ec, value := server.Read(key)
			if ec == NOT_LEADER {
				leaderId := server.leaderId
				if leaderId == "" {
					randomNode := nodeIDs[rand.Intn(len(nodeIDs))]
					leaderId = randomNode
				}
				send(server.id, leaderId, map[string]interface{}{
					"type":  "read",
					"key":   body["key"],
					"value": body["value"],
				}, &msg)
			}

			if value == nil || value == "" {
				if originalMsg != nil {
					reply(*originalMsg, map[string]interface{}{
						"type":  "read_ok",
						"value": nil,
					})
				} else {
					reply(msg, map[string]interface{}{
						"type":  "read_ok",
						"value": nil,
					})
				}
				break
			}

			intValue, err := strconv.Atoi(value.(string))
			if err != nil {
				// Handle the error (e.g., return an error response or log it)
				reply(msg, map[string]interface{}{
					"type": "error",
					"text": "Failed to parse value as integer",
				})
				break
			}

			if originalMsg != nil {
				reply(*originalMsg, map[string]interface{}{
					"type":  "read_ok",
					"value": intValue,
				})
				break
			}
			reply(msg, map[string]interface{}{
				"type":  "read_ok",
				"value": intValue,
			})
			break

		case "append_entries":
			println("Received append entry")

			// Check 'term'
			term, ok := body["term"].(float64)
			if !ok {
				log.Printf("Missing or invalid 'term' in append_entries message")
				continue
			}

			// Check 'leader_id'
			leaderID, ok := body["leader_id"].(string)
			if !ok {
				log.Printf("Missing or invalid 'leader_id' in append_entries message")
				continue
			}

			// Check 'prev_log_index'
			prevLogIndex, ok := body["prev_log_index"].(float64)
			if !ok {
				log.Printf("Missing or invalid 'prev_log_index' in append_entries message")
				continue
			}

			// Check 'prev_log_term'
			prevLogTerm, ok := body["prev_log_term"].(float64)
			if !ok {
				log.Printf("Missing or invalid 'prev_log_term' in append_entries message")
				continue
			}

			// Check 'leader_commit'
			leaderCommit, ok := body["leader_commit"].(float64)
			if !ok {
				log.Printf("Missing or invalid 'leader_commit' in append_entries message")
				continue
			}

			// Process entries (already safely handled in processEntries)
			entriesRaw := body["entries"]
			entries := processEntries(entriesRaw)

			// Construct the AppendEntriesRequest
			ap := AppendEntriesRequest{
				Term:         int(term),
				LeaderID:     leaderID,
				PrevLogIndex: int(prevLogIndex),
				PrevLogTerm:  int(prevLogTerm),
				Entries:      entries,
				LeaderCommit: int(leaderCommit),
			}

			// Call AppendEntries and reply
			respBody := server.AppendEntries(ap)
			respBody["type"] = "append_entries_ok"
			send(server.id, msg.Src, respBody, originalMsg)
			break

		case "append_entries_ok":
			followerID := msg.Src
			success := body["success"].(bool)
			term := int(body["term"].(float64))

			errType, newMsg, _ := server.WaitForReplication(followerID, success, term)

			// ACHO QUE VAI TER UM BUG POR PODER MANDAR VÁRIOS WRITE OK AO MM CLIENTE
			if errType == ERROR && newMsg != nil {
				println("ERROR ON WAIT REPLICATION")
				send(nodeID, followerID, map[string]interface{}{
					"type":           "append_entries",
					"term":           newMsg.Term,
					"leader_id":      newMsg.LeaderID,
					"prev_log_index": newMsg.PrevLogIndex,
					"prev_log_term":  newMsg.PrevLogTerm,
					"entries":        newMsg.Entries,
					"leader_commit":  newMsg.LeaderCommit,
				}, originalMsg)
			} else {
				if originalMsg == nil {
					println("NO ORIGINAL MESSAGE")
					// era apenas um heartbeat ent pode-se ignorar
					break
				}
				println("WRITE SUCCESS")
				reply(*originalMsg, map[string]interface{}{
					"type": "write_ok",
				})
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
