package MsgProtocol

import (
	"encoding/json"
	"fmt"
)

const (
	ConnHostName      = "fa19-cs425-g73-%02d.cs.illinois.edu"
	ConnType          = "udp"
	ConnPort          = "8888"
	HeartbeatPort     = "8887"
	IntroducePort     = "8886"
	ConnlocalHost     = "localhost"
	TimeOut           = 4100
	IntroducerAddress = "fa19-cs425-g73-01.cs.illinois.edu"
)

const (
	HeartbeatMsg = "Heartbeat"
	JoinMsg      = "Join" //Content is empty
	LeaveMsg     = "Leave"
	FailMsg      = "Fail"
	IntroduceMsg = "Introduce" // Content will include new-join node's ID
	JoinAckMsg   = "JoinAck"   // Content will include full membership list
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
