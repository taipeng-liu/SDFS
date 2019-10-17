package helper

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
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

//Used for debugging
func PrintMsg(msg Message) {
	fmt.Printf("MessageType: %s... NodeID: %s", msg.MessageType, msg.NodeID)
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
	//fmt.Println(b)
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

func CloseConnPort(localID string) {

	//Send Leave Msg to local listener to close connection
	leaveMsg := NewMessage(LeaveMsg, localID, []string{localID})
	leavePkg := MsgToJSON(leaveMsg)

	udpAddr, err := net.ResolveUDPAddr(ConnType, ":"+ConnPort)
	if err != nil {
		log.Println(err.Error())
	}

	conn, err := net.DialUDP(ConnType, nil, udpAddr)
	if err != nil {
		log.Println(err.Error())
		// os.Exit(1)
	}

	_, err = conn.Write(leavePkg)
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}
	conn.Close()
	fmt.Println("Conn Port Closed!!")

}

func CloseIntroducePort(localID string) {

	//Send Leave Msg to local listener to close connection
	leaveMsg := NewMessage(LeaveMsg, localID, []string{})
	leavePkg := MsgToJSON(leaveMsg)

	udpAddr, err := net.ResolveUDPAddr(ConnType, ":"+IntroducePort)
	if err != nil {
		log.Println(err.Error())
	}

	conn, err := net.DialUDP(ConnType, nil, udpAddr)
	if err != nil {
		log.Println(err.Error())
		// os.Exit(1)
	}
	_, err = conn.Write(leavePkg)
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	conn.Close()
	fmt.Println("Introducer Port Closed!!")

}

func CloseHBPort(localID string) {

	//Send Leave Msg to local listener to close connection
	leaveMsg := NewMessage(LeaveMsg, localID, []string{localID})
	leavePkg := MsgToJSON(leaveMsg)

	udpAddr, err := net.ResolveUDPAddr(ConnType, ":"+HeartbeatPort)
	if err != nil {
		log.Println(err.Error())
	}

	conn, err := net.DialUDP(ConnType, nil, udpAddr)
	if err != nil {
		log.Println(err.Error())
		// os.Exit(1)
	}
	_, err = conn.Write(leavePkg)
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}
	conn.Close()
	fmt.Println("HB Port Closed!!")

}
