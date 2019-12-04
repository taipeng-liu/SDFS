package membership

import (
	"fmt"
	"log"
	"net"
	"time"

	MP "../MsgProtocol"
	Config "../Config"
	Conn "../Connection"
)

// Sender is a type that implements the SendHearbeat() "method"
type Sender struct{}

//Join the group
func (s *Sender) SendJoin() bool{
	joinSucceed := SendJoinMsg(Config.IntroducerAddress)

	if !joinSucceed {
		fmt.Println("Introducer is down!!")
	}
	return joinSucceed
}

func SendJoinMsg(introducerAddress string) bool {
	joinMsg := MP.NewMessage(MP.JoinMsg, LocalID, []string{})
	joinPkg := MP.MsgToJSON(joinMsg)

	conn := Conn.BuildUDPClient(introducerAddress, Config.IntroducePort)
	defer conn.Close()
	Conn.WriteToUDPConn(joinPkg, conn)
	log.Println("Sender: JoinMsg Sent to Introducer...")

	//Set 3s Deadline for Ack
	conn.SetReadDeadline(time.Now().Add(time.Duration(3) * time.Second))
	n, joinAck := Conn.ReadUDP(conn)
	if n == -1 {
		return false
	}
	joinAckMsg := MP.JSONToMsg([]byte(string(joinAck[:n])))

	log.Println("Sender: Checking joinAck from Introducer")

	if joinAckMsg.MessageType == MP.JoinAckMsg {
		log.Println("Sender: Receive JoinAck, Join the Group!")
		fmt.Println("Successfully join the group...")
		UpdateMemshipList(joinAckMsg)
		return true
	} else {
		log.Println("Sender: Received Wrong Ack, join fails...")
		return false
	}
}

func (s *Sender) SendLeave() {
	isIntroducer := Config.IsIntroducer()
	if isIntroducer {
		Conn.CloseLocalPort(LocalID, Config.IntroducePort)
	}
	Conn.CloseLocalPort(LocalID, Config.HeartbeatPort)
	Conn.CloseLocalPort(LocalID, Config.ConnPort)
}

func (s *Sender) SendHeartbeat() {
	heartBeatMsg := MP.NewMessage(MP.HeartbeatMsg, LocalID, []string{})
	heartBeatPkg := MP.MsgToJSON(heartBeatMsg)

	for {
		select {
		case <-KillHBSender:
			fmt.Println("====Heartbeat Sender: Leave!!")
			return
		default:
			for _, monitorID := range MonitorList {
				monitorAddress := Config.GetIPAddressFromID(monitorID)

				conn := Conn.BuildUDPClient(monitorAddress, Config.HeartbeatPort)
				Conn.WriteToUDPConn(heartBeatPkg, conn)
				//log.Printf("===HeartBeat Message Sent to Monitor: %s !!!\n", monitorID)
				conn.Close()
			}
			time.Sleep(300 * time.Millisecond) //send heartbeat 1 second
		}
	}

}

func SendMsgToAddress(msg MP.Message, addr string, port string, ln *net.UDPConn) {
	pkg := MP.MsgToJSON(msg)
	udpAddr, err := net.ResolveUDPAddr("udp", addr + ":" + port)
	if err != nil {
		log.Println(err.Error())
	}
	
	_, wErr := ln.WriteToUDP(pkg, udpAddr)
	if wErr != nil {
		log.Println(wErr.Error())
	}
	log.Printf("Sender: Sent %s to %s...\n",msg.MessageType, addr)
}

func sendMsgToAllMonitors(msg MP.Message, predecessorID string, ln *net.UDPConn) {
	pkg := MP.MsgToJSON(msg)
	for _, monitorID := range MonitorList {
		if monitorID == predecessorID {
			continue
		}
		monitorAddress := Config.GetIPAddressFromID(monitorID)

		//SendMsgToAddress(msg, monitorAddress, Config.ConnPort, ln)

		udpAddr, err := net.ResolveUDPAddr(Config.ConnType, monitorAddress + ":" + Config.ConnPort)
		if err != nil {
			log.Println(err.Error())
		}
		
		_, wErr := ln.WriteToUDP(pkg, udpAddr)
		if wErr != nil {
			log.Println(wErr.Error())
		}
		log.Printf("Sender:Sent %s to Monitor: %s...\n", msg.MessageType, monitorID)
	}
}

func sendMsg(ln *net.UDPConn, preID string, msgType string, contentID string) {
	msg := MP.NewMessage(msgType, LocalID, []string{contentID})
	sendMsgToAllMonitors(msg, preID, ln)
}

func SendLeaveMsg(ln *net.UDPConn, predecessorID string, leaveNodeID string) {
	sendMsg(ln, predecessorID, MP.LeaveMsg, leaveNodeID)
}

func SendIntroduceMsg(ln *net.UDPConn, predecessorID string, newNodeID string) {
	sendMsg(ln, predecessorID, MP.IntroduceMsg, newNodeID)
}

func SendFailMsg(ln *net.UDPConn, predecessorID string, failNodeID string) {
	sendMsg(ln, predecessorID, MP.FailMsg, failNodeID)
}
