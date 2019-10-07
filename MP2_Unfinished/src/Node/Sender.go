package node

import (
	"fmt"
	"log"
	"net"

	msg "../Helper"

	//"os/exec"
	"time"
	//"strings"
)

// Sender is a type that implements the SendHearbeat() "method"
type Sender struct{}

//Join the group
func (s *Sender) SendJoin() {
	joinSucceed := SendJoinMsg(msg.IntroducerAddress)

	if !joinSucceed {
		fmt.Println("Introducer is down!!")
		return
	}
	return
}

func (s *Sender) SendLeave() {
	// var membershipList []string
	// var monitorList []string
	// localHostName := msg.GetHostName()
	// UpQryChan <- UpdateQuery{0, ""}
	// membershipList =<- MemListChan
	isIntroducer := msg.IsIntroducer()
	if isIntroducer {
		fmt.Println("Close Introducer Port")
		msg.CloseIntroducePort(LocalID)
	}
	msg.CloseHBPort(LocalID)
	msg.CloseConnPort(LocalID)
	// KillMsgListener <- struct{}{}

	fmt.Println("All Port Closed!!")
	return

}

func (s *Sender) SendHeartbeat() {
	heartBeatMsg := msg.NewMessage(msg.HeartbeatMsg, LocalID, []string{})
	heartBeatPkg := msg.MsgToJSON(heartBeatMsg)

	for {
		select {
		case <-KillHBSender:
			// ln.Close()
			fmt.Println("====Heartbeat Sender: Leave!!")
			// KillRoutine <- struct{}{}
			return

		default:
			for _, monitorID := range MonitorList {
				monitorAddress := msg.GetIPAddressFromID(monitorID)
				udpAddr, err := net.ResolveUDPAddr(msg.ConnType, monitorAddress+":"+msg.HeartbeatPort)
				if err != nil {
					log.Println(err.Error())
					// os.Exit(1)
				}
				conn, err := net.DialUDP(msg.ConnType, nil, udpAddr)
				if err != nil {
					log.Println(err.Error())
					// os.Exit(1)
				}

				_, err = conn.Write(heartBeatPkg)
				if err != nil {
					log.Println(err.Error())
				}

				log.Printf("===HeartBeat Message Sent to Monitor: %s !!!\n", monitorID)
				conn.Close()
			}
			time.Sleep(300 * time.Millisecond) //send heartbeat 1 second
		}
	}

}

func SendJoinMsg(introducerAddress string) bool {
	joinMsg := msg.NewMessage(msg.JoinMsg, LocalID, []string{})
	joinPkg := msg.MsgToJSON(joinMsg)

	udpAddr, err := net.ResolveUDPAddr(msg.ConnType, introducerAddress+":"+msg.IntroducePort)
	if err != nil {
		log.Println(err.Error())
		// os.Exit(1)
	}
	conn, err := net.DialUDP(msg.ConnType, nil, udpAddr)
	if err != nil {
		log.Println(err.Error())
		// os.Exit(1)
	}
	defer conn.Close()

	_, err = conn.Write(joinPkg)
	if err != nil {
		log.Println(err.Error())
		// os.Exit(1)
	}
	log.Println("Sender: JoinMsg Sent to Introducer...")

	//Set 3s Deadline for Ack
	conn.SetReadDeadline(time.Now().Add(time.Duration(3) * time.Second))

	//Read from Introducer
	joinAck := make([]byte, 2048)
	n, err := conn.Read(joinAck)
	if err != nil {
		log.Println(err.Error())
		return false
	}

	joinAckMsg := msg.JSONToMsg([]byte(string(joinAck[:n])))

	log.Printf("Sender: JoinAckMsg Received from Introducer, the message type is: %s...: ", joinAckMsg.MessageType)

	if joinAckMsg.MessageType == msg.JoinAckMsg {
		UpdateMemshipList(joinAckMsg)
		//MembershipList = joinAckMsg.Content
		//UpdateMemHBMap()

		return true
	} else {
		log.Println("Sender: Received Wrong Ack...")
		return false
	}
	return true
}

func SendLeaveMsg(ln *net.UDPConn, predecessorID string, leaveNodeID string) {

	leaveMsg := msg.NewMessage(msg.LeaveMsg, LocalID, []string{leaveNodeID})
	leavePkg := msg.MsgToJSON(leaveMsg)
	log.Println("===Sender: MembershipList is")
	log.Print(MonitorList)

	for _, member := range MonitorList {

		if member == predecessorID {
			//Predecessor is the node who sends me LeaveMsg
			//I won't send back this msg to it.!!!!!!
			continue
		}

		memberAddress := msg.GetIPAddressFromID(member)
		fmt.Println("===Listener: Monitor is" + memberAddress)
		udpAddr, err := net.ResolveUDPAddr(msg.ConnType, memberAddress+":"+msg.ConnPort)
		if err != nil {
			log.Println(err.Error())
		}

		conn, err := net.DialUDP(msg.ConnType, nil, udpAddr)
		if err != nil {
			log.Println(err.Error())
			// os.Exit(1)
		}

		defer conn.Close()

		_, wErr := ln.WriteToUDP(leavePkg, udpAddr)
		if wErr != nil {
			log.Println(wErr.Error())
		}
		log.Printf("Sender:LeaveMsg Sent to Monitor: %s...\n", member)
	}
	return

}

func SendIntroduceMsg(ln *net.UDPConn, predecessorID string, newNodeID string) {
	introduceMsg := msg.NewMessage(msg.IntroduceMsg, LocalID, []string{newNodeID})
	introducePkg := msg.MsgToJSON(introduceMsg)
	//monitorList := msg.GetMonitorList(MembershipList, LocalAddress)

	for _, member := range MonitorList {
		// fmt.Println(i,member)
		if member == predecessorID {
			//My predecessor is the one who send me the failMsg
			//I will not send back!!!!!!!!!!!!!
			continue
		}

		memberAddress := msg.GetIPAddressFromID(member)
		udpAddr, err := net.ResolveUDPAddr(msg.ConnType, memberAddress+":"+msg.ConnPort)
		if err != nil {
			log.Println(err.Error())
		}

		_, wErr := ln.WriteToUDP(introducePkg, udpAddr)
		if wErr != nil {
			log.Println(wErr.Error())
		}
		log.Println("Sender:IntroduceMsg Sent to: " + member)
	}
}

func SendFailMsg(ln *net.UDPConn, predecessorID string, failNodeID string) {

	failMsg := msg.NewMessage(msg.FailMsg, LocalID, []string{failNodeID})
	failPkg := msg.MsgToJSON(failMsg)
	fmt.Printf("Sender: Node %s is failed...\n ", failNodeID)

	//monitorList := msg.GetMonitorList(MembershipList, LocalAddress)

	for _, member := range MonitorList {
		if member == predecessorID {
			//My predecessor is the one who send me the failMsg
			//I will not send back!!!!!!!!!!!!!
			continue
		}

		memberAddress := msg.GetIPAddressFromID(member)

		udpAddr, err := net.ResolveUDPAddr(msg.ConnType, memberAddress+":"+msg.ConnPort)
		if err != nil {
			log.Println(err.Error())
		}

		_, wErr := ln.WriteToUDP(failPkg, udpAddr)
		if wErr != nil {
			log.Println(wErr.Error())
		}
		log.Printf("Sender: FailMsg Sent to Monitor: %s...\n ", memberAddress)
	}

	return

}
