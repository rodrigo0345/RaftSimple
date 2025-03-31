package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
)

func followerToCandidate(msg map[string]interface{}) {
	println("Sending heartbeat")
	msg["type"] = "request_vote"
	for _, node := range nodeIDs {
		if node == nodeID {
			continue
		}
		send(nodeID, node, msg, nil)
	}
}

func leaderHeartbeat(msg map[string]interface{}) {
	println("Sending heartbeat")
	msg["type"] = "append_entries"
	for _, node := range nodeIDs {
		if node == nodeID {
			continue
		}
		send(nodeID, node, msg, nil)
	}
}

func candidateStartNewElection(msg map[string]interface{}) {
	println("Sending heartbeat")
	msg["type"] = "request_vote"
	for _, node := range nodeIDs {
		if node == nodeID {
			continue
		}
		send(nodeID, node, msg, nil)
	}
}

func main() {
	var server *Server
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
			server = NewServer(nodeID, nodeIDs, followerToCandidate, leaderHeartbeat, candidateStartNewElection)
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
					randomNode := nodeIDs[rand.Intn(len(nodeIDs))]
					leaderId = randomNode
				}
				send(server.id, leaderId, body, &msg)
			}

			// if error might mean something that I did not account for
			if msgType == ERROR {
				break
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

				broadcast(server, appendEntriesRequest, originalMsg)
			}
			break

		case "read":
			keyFloat := body["key"].(float64)
			key := fmt.Sprintf("%v", keyFloat)
			ec, value := server.Read(key)
			if ec == NOT_LEADER {
				send(server.id, server.leaderId, map[string]interface{}{
					"type":  "read",
					"key":   body["key"],
					"value": body["value"],
				}, &msg)
			}
			if originalMsg != nil {
				reply(*originalMsg, map[string]interface{}{
					"type":  "read_ok",
					"key":   key,
					"value": value,
				})
				break
			}
			reply(msg, map[string]interface{}{
				"type":  "read_ok",
				"key":   key,
				"value": value,
			})
			break

		case "append_entries":
			println("Received append entry")
			entriesRaw := body["entries"]
			entries := processEntries(entriesRaw)
			ap := AppendEntriesRequest{
				Term:         int(body["term"].(float64)),
				LeaderID:     body["leader_id"].(string),
				PrevLogIndex: int(body["prev_log_index"].(float64)),
				PrevLogTerm:  int(body["prev_log_term"].(float64)),
				Entries:      entries,
				LeaderCommit: int(body["leader_commit"].(float64)),
			}
			body := server.AppendEntries(ap)
			body["type"] = "append_entries_ok"
			reply(msg, body)
			break

		case "append_entries_ok":
			followerID := msg.Src
			success := body["success"].(bool)
			term := int(body["term"].(float64))
			errType, newMsg, _ := server.WaitForReplication(followerID, success, term)

			// ACHO QUE VAI TER UM BUG POR PODER MANDAR VÁRIOS WRITE OK AO MM CLIENTE
			if errType == ERROR && newMsg != nil {
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
			voteGranted := body["vote_granted"].(bool)
			term := int(body["term"].(float64))
			voterID := msg.Src
			server.candidate.HandleVoteResponse(voterID, term, voteGranted)

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
}
