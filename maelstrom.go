package main

import (
	"encoding/json"
	"fmt"
	"log"
)

var msgID int = 0
var nodeID string = "0"
var nodeIDs []string = []string{}

type MessageInternal struct {
	Src  string          `json:"src"`
	Dest string          `json:"dest"`
	Body json.RawMessage `json:"body"`
}

type BaseBody struct {
	MsgID int `json:"msg_id"`
}

type InitBody struct {
	MsgID   int      `json:"msg_id"`
	NodeIDs []string `json:"node_ids"`
	NodeID  string   `json:"node_id"`
}

func send(src, dest string, body map[string]interface{}, originalMsg *MessageInternal) {
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
	if originalMsg != nil {
		data["body"].(map[string]interface{})["original_msg"] = originalMsg
	}
	jsonData, _ := json.Marshal(data)
	log.Printf("sending %s", jsonData)
	fmt.Println(string(jsonData))
}

func reply(originalMsg MessageInternal, body map[string]interface{}) {
	var origBody map[string]interface{}
	if err := json.Unmarshal(originalMsg.Body, &origBody); err != nil {
		log.Printf("Error unmarshaling original body: %v", err)
		return
	}
	responseBody := map[string]interface{}{
		"in_reply_to": uint64(origBody["msg_id"].(float64)),
	}
	for k, v := range body {
		responseBody[k] = v
	}
	send(nodeID, originalMsg.Src, responseBody, nil)
}

func isInitialized() bool {
	return len(nodeIDs) > 0
}

func getNodeID() string {
	return nodeID
}

func initNode(msg MessageInternal) {
	var initBody InitBody
	if err := json.Unmarshal(msg.Body, &initBody); err != nil {
		log.Printf("Error unmarshaling init body: %v", err)
		return
	}
	nodeIDs = initBody.NodeIDs
	nodeID = initBody.NodeID
	log.Printf("init: nodeIDs=%v, nodeID=%v", nodeIDs, nodeID)
}

func DecodeMessage(msg string) (MessageInternal, map[string]interface{}, *MessageInternal) {
	// Define a temporary struct to hold all fields
	var fullMsg struct {
		MessageInternal
		OriginalMsg *MessageInternal `json:"original_msg"`
	}

	// Unmarshal the JSON string into fullMsg
	if err := json.Unmarshal([]byte(msg), &fullMsg); err != nil {
		log.Printf("Error unmarshaling message: %v", err)
		panic("Error unmarshaling message in DecodeMessage")
	}

	var bodyMap map[string]interface{}
	if err := json.Unmarshal(fullMsg.Body, &bodyMap); err != nil {
		log.Printf("Error unmarshaling body: %v", err)
		panic("Error unmarshaling message in DecodeMessage")
	}

	// Safely check if original_msg exists in bodyMap
	var originalMsg *MessageInternal
	if val, ok := bodyMap["original_msg"]; ok {
		if casted, ok := val.(*MessageInternal); ok {
			originalMsg = casted
		} else {
			log.Printf("Type assertion failed for original_msg: %v", val)
			originalMsg = nil // Default to nil if type assertion fails
		}
	} else {
		originalMsg = nil // Return nil if original_msg is not present
	}

	// Return the embedded MessageInternal as Header,
	// the Body as json.RawMessage, and the OriginalMsg as *MessageInternal
	return fullMsg.MessageInternal, bodyMap, originalMsg
}
