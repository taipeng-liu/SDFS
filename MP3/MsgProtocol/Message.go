package MsgProtocol

import (
	"encoding/json"
	"fmt"
)

const (
	HeartbeatMsg = "Heartbeat"
	JoinMsg      = "Join" //Content is empty
	LeaveMsg     = "Leave"
	FailMsg      = "Fail"
	IntroduceMsg = "Introduce" // Content will include new-join node's ID
	JoinAckMsg   = "JoinAck"   // Content will include full membership list
	IntroducerRejoinMsg = "IntroducerRejoin" //Content is empty
)

type Message struct {
	MessageType string   //Heartbeat, Join, Leave, Introduce,(IntroduceAck?)
	NodeID      string   //Local ID
	Content     []string //Message Content
}

func NewMessage(Type string, ID string, Content []string) Message {
	newMessage := Message{
		MessageType: Type,
		NodeID:      ID,
		Content:     Content,
	}
	return newMessage
}

func MsgToJSON(message Message) []byte {
	b, err := json.Marshal(message)
	if err != nil {
		fmt.Println(err)
	}
	return b
}

func JSONToMsg(b []byte) Message {
	var m Message
	err := json.Unmarshal(b, &m)
	if err != nil {
		fmt.Println(err)
	}
	return m
}
