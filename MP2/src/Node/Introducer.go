package node

import (
	"fmt"
	"log"
	"net"

	// "os"
	msg "../Helper"
)

type Introducer struct{}

//Called from Node.go when the node type is Introducer
func (i *Introducer) NodeHandleJoin() {
	//Add Introducer itself to MemList
	ok := AddNode(LocalID)
	if !ok {
		fmt.Printf("Introducer %s already exits\n", LocalID)
		return
	}

	fmt.Println("Introducer: Start Listening for New-Join Node...")
	ln := BuildUDPServer(msg.IntroducePort)

	//Handle JoinMsg
	for {
		select {
		case <-KillIntroducer:
			ln.Close()
			fmt.Println("====Introducer: Leave!!")
			// KillRoutine <- struct{}{}
			return
		default:
			fmt.Println("====Introducer: Works!!")
			HandleJoinMsg(ln)
		}
	}

}

func HandleJoinMsg(ln *net.UDPConn) {
	joinBuf := make([]byte, 1024)
	n, joinAddr, err := ln.ReadFromUDP(joinBuf)
	if err != nil {
		log.Println(err.Error())
	}

	joinMsg := msg.JSONToMsg([]byte(string(joinBuf[:n])))

	if joinMsg.MessageType == msg.JoinMsg {
		log.Printf("Introducer: JoinMsg Received from Node: %s...\n", joinMsg.NodeID)
		

		//Send Introduce Message to Other node
		SendIntroduceMsg(ln, "", joinMsg.NodeID)

		UpdateMemshipList(joinMsg)

		//Send full membershiplist to new join node
		joinAckMsg := msg.NewMessage(msg.JoinAckMsg, LocalID, MembershipList)
		joinAckPkg := msg.MsgToJSON(joinAckMsg)

		_, err := ln.WriteToUDP(joinAckPkg, joinAddr)
		if err != nil {
			log.Println(err.Error())
		}
		log.Printf("Introducer: JoinAck Sent to Node: %s...\n", joinMsg.NodeID)
	} else if joinMsg.MessageType == msg.LeaveMsg {
		log.Printf("Introducer: Introducer Leave... Close Port:%s...\n", msg.IntroducePort)
	}
	return
}
