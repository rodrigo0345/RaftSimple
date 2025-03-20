package main

import (
	"encoding/json"
	"fmt"
	"log"
)

var msgID int = 0
var nodeID string = "0"
var nodeIDs []string = []string{}

type Message struct {
	Src  string `json:"src"`
	Dest string `json:"dest"`
	Body struct {
		MsgID int `json:"msg_id"`
    Type string `json:"type"`
    NodeIDs []string `json:"node_ids,omitempty"`
    NodeID string `json:"node_id,omitempty"`
	} `json:"body"`
}

func send(src, dest string, body map[string]interface{}) {
	msgID++
	data := map[string]interface{}{
		"src":  src,
		"dest": dest,
		"body": map[string]interface{}{
			"msg_id": msgID,
		},
	}
	for k, v := range body {
		data["body"].(map[string]interface{})[k] = v
	}
	jsonData, _ := json.Marshal(data)
	log.Printf("sending %s", jsonData)
	fmt.Println(string(jsonData))
}

func reply(msg Message, body map[string]interface{}) {
	body["in_reply_to"] = msg.Body.MsgID
	send(msg.Dest, msg.Src, body)
}

func isInitialized() bool {
  return len(nodeIDs) > 0
}

func getNodeID() string {
  return nodeID
}

func initNode(msg Message) {
  nodeIDs = msg.Body.NodeIDs
  nodeID = msg.Body.NodeID
  log.Printf("init: nodeIDs=%v, nodeID=%v", nodeIDs, nodeID)
}
