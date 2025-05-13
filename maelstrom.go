package main

import (
	"encoding/json"
	"fmt"
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
	if src == dest {
		// log.Printf("Skipping self-send from %s to %s: %v", src, dest, body)
		return
	}
	msgID++
	data := map[string]interface{}{
		"body": map[string]interface{}{
			"msg_id": msgID,
		},
		"src":  src,
		"dest": dest,
	}
	// Copy fields from the provided body
	for k, v := range body {
		data["body"].(map[string]interface{})[k] = v
	}
	// If originalMsg is provided, include it without its own original_msg field
	if originalMsg != nil {
		var origBody map[string]interface{}
		if err := json.Unmarshal(originalMsg.Body, &origBody); err != nil {
			// log.Printf("Error unmarshaling originalMsg body: %v", err)
		} else {
			delete(origBody, "original_msg")
			cleanedOriginalMsg := MessageInternal{
				Src:  originalMsg.Src,
				Dest: originalMsg.Dest,
				Body: json.RawMessage{},
			}
			cleanedBody, err := json.Marshal(origBody)
			if err != nil {
				// log.Printf("Error marshaling cleaned originalMsg body: %v", err)
			} else {
				cleanedOriginalMsg.Body = cleanedBody
				data["body"].(map[string]interface{})["original_msg"] = cleanedOriginalMsg
			}
		}
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		// log.Printf("Error marshaling message: %v", err)
		return
	}
	// log.Printf("sending %s", jsonData)
	fmt.Println(string(jsonData))
}

func reply(originalMsg MessageInternal, body map[string]interface{}) {
	var origBody map[string]interface{}
	if err := json.Unmarshal(originalMsg.Body, &origBody); err != nil {
		// log.Printf("Error unmarshaling original body: %v", err)
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
		// log.Printf("Error unmarshaling init body: %v", err)
		return
	}
	nodeIDs = initBody.NodeIDs
	nodeID = initBody.NodeID
	// log.Printf("init: nodeIDs=%v, nodeID=%v", nodeIDs, nodeID)
}

func DecodeMessage(msg string) (MessageInternal, map[string]interface{}, *MessageInternal) {
	var fullMsg struct {
		Src  string          `json:"src"`
		Dest string          `json:"dest"`
		Body json.RawMessage `json:"body"`
	}
	if err := json.Unmarshal([]byte(msg), &fullMsg); err != nil {
		// log.Printf("Error unmarshaling message: %v", err)
		panic("Error unmarshaling message in DecodeMessage")
	}

	var bodyMap map[string]interface{}
	if err := json.Unmarshal(fullMsg.Body, &bodyMap); err != nil {
		// log.Printf("Error unmarshaling body: %v", err)
		panic("Error unmarshaling message in DecodeMessage")
	}

	var originalMsg *MessageInternal
	if val, ok := bodyMap["original_msg"]; ok {
		origMsgBytes, err := json.Marshal(val)
		if err != nil {
			// log.Printf("Error marshaling original_msg: %v", err)
		} else {
			var origMsg MessageInternal
			if err := json.Unmarshal(origMsgBytes, &origMsg); err != nil {
				// log.Printf("Error unmarshaling original_msg: %v", err)
			} else {
				originalMsg = &origMsg
			}
		}
	}

	header := MessageInternal{
		Src:  fullMsg.Src,
		Dest: fullMsg.Dest,
		Body: fullMsg.Body,
	}
	return header, bodyMap, originalMsg
}
