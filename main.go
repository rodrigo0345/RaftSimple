package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
)

var server *Server
var byzantineMode bool

func followerToCandidate(msg map[string]interface{}) {
	msg["type"] = "request_vote"
	for _, node := range nodeIDs {
		if node == nodeID {
			continue
		}
		send(nodeID, node, msg, nil)
	}
}

func leaderHeartbeat(msg map[string]interface{}) {
	msg["type"] = "append_entries"
	for _, node := range nodeIDs {
		if node == nodeID {
			continue
		}
		send(nodeID, node, msg, nil)
	}
}

func candidateStartNewElection(msg map[string]interface{}) {
	msg["type"] = "request_vote"
	for _, node := range nodeIDs {
		if node == nodeID {
			continue
		}
		send(nodeID, node, msg, nil)
	}
}

func main() {
	flag.BoolVar(&byzantineMode, "byzantine", false, "Enable Byzantine AppendEntries fault")
	flag.Parse()

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
		case "cas":
			keyFloat := body["key"].(float64)
			fromValueFloat := body["from"].(float64)
			toValueFloat := body["to"].(float64)
			key := fmt.Sprintf("%v", keyFloat)
			fromValue := fmt.Sprintf("%v", fromValueFloat)
			toValue := fmt.Sprintf("%v", toValueFloat)

			originalMsg := &msg
			if clientMsg != nil {
				originalMsg = clientMsg
			}

			msgFrom := ""
			if strings.Contains(msg.Src, "n") {
				msgFrom = msg.Src
			}

			msgType, appendReq, opResponse := server.Cas(key, fromValue, toValue, originalMsg, msgFrom)
			leaderId := server.leaderId

			if msgType == NOT_LEADER {
				if leaderId == "" {
					leaderId = selectRandomLeader(server)
				}
				forwardBody := make(map[string]interface{})
				for k, v := range body {
					forwardBody[k] = v
				}
				send(server.id, leaderId, forwardBody, &msg)
				break
			}

			if msgType == CAS_OK {
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
			} else {
				reply(*originalMsg, map[string]interface{}{
					"type": "error",
					"code": int(body["code"].(float64)),
					"text": opResponse,
				})
			}
		case "write":
			keyFloat := body["key"].(float64)
			valueFloat := body["value"].(float64)
			key := fmt.Sprintf("%v", keyFloat)
			value := fmt.Sprintf("%v", valueFloat)

			originalMsg := &msg
			if clientMsg != nil {
				originalMsg = clientMsg
			}

			msgFrom := ""
			if strings.Contains(msg.Src, "n") {
				msgFrom = msg.Src
			}

			msgType, appendReq, leaderId := server.Write(key, value, originalMsg, msgFrom)

			if msgType == NOT_LEADER {
				if leaderId == "" {
					leaderId = selectRandomLeader(server)
				}
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

				broadcast(server, appendEntriesRequest, originalMsg)
			}
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
		case "append_entries":
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
			if clientMsg != nil {
				send(server.id, msg.Src, respBody, clientMsg)
			} else {
				send(server.id, msg.Src, respBody, nil)
			}
		case "append_entries_ok":
			followerID := msg.Src
			success := body["success"].(bool)
			term := int(body["term"].(float64))

			errType, newMsg, confirmedOps := server.WaitForReplication(followerID, success, term)
			if errType == NOT_LEADER {
				break
			}
			if errType == RETRY_SEND {
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
				for _, op := range confirmedOps {
					if op.ClientMessage == nil {
						continue
					}
					reply(*op.ClientMessage, op.Response)
				}
			}
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
		case "request_vote_ok":
			server.Lock()
			voteGranted := body["vote_granted"].(bool)
			term := int(body["term"].(float64))
			voterID := msg.Src
			server.candidate.HandleVoteResponse(server, voterID, term, voteGranted)
			server.Unlock()
		case "log_digest":
			server.Lock()
			server.HandleLogDigest(body, msg.Src)
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
		msgCopy := make(map[string]interface{})
		for k, v := range msg {
			if k == "entries" {
				entriesRaw, _ := json.Marshal(v)
				var entries []LogEntry
				json.Unmarshal(entriesRaw, &entries)
				for i := range entries {
					var prevHash [32]byte
					if entries[i].Index > 0 && len(server.log) > entries[i].Index-1 {
						prevHash = server.log[entries[i].Index-1].Cumulative
					}
					command := entries[i].Command
					if byzantineMode && node == "n2" {
						command = command + "_byzantine"
						log.Printf("[Broadcast] Byzantine mode: modified command for n2 to %s", command)
					}
					hashInput := append(prevHash[:], []byte(fmt.Sprintf("%d|%s", entries[i].Term, command))...)
					entries[i].Cumulative = sha256.Sum256(hashInput)
					log.Printf("[Broadcast] Computed hash for node %s entry index=%d: %x", node, entries[i].Index, entries[i].Cumulative[:8])
					// Update leader's log with non-Byzantine hash
					if node != "n2" || !byzantineMode {
						if entries[i].Index < len(server.log) {
							server.log[entries[i].Index].Cumulative = entries[i].Cumulative
							log.Printf("[Broadcast] Updated leader's log index=%d with hash: %x", entries[i].Index, entries[i].Cumulative[:8])
						}
					}
				}
				msgCopy[k] = entries
			} else {
				msgCopy[k] = v
			}
		}
		send(server.id, node, msgCopy, originalMessage)
	}
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
