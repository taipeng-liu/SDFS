package membership

import (
	"fmt"
	"log"
	"net"

	MP "../MsgProtocol"
	Conn "../Connection"
)

type Introducer struct{}

func (i *Introducer) NodeHandleJoin() {
	//Add Introducer itself to MemList
	ok := AddNode(LocalID)
	if !ok {
		return
	}

	fmt.Println("Introducer: Start Listening for New-Join Node...")
	ln := Conn.BuildUDPServer(MP.IntroducePort)

	//Handle JoinMsg
	for {
		select {
		case <-KillIntroducer:
			ln.Close()
			fmt.Println("====Introducer: Leave!!")
			return
		default:
			fmt.Println("====Introducer: Waiting for new join...")
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

	joinMsg := MP.JSONToMsg([]byte(string(joinBuf[:n])))

	if joinMsg.MessageType == MP.JoinMsg {
		log.Printf("Introducer: JoinMsg Received from Node: %s...\n", joinMsg.NodeID)
		fmt.Printf("====Introducer: JoinMsg Received from Node: %s...\n", joinMsg.NodeID)
		
		//Send Introduce Message to Other node
		SendIntroduceMsg(ln, "", joinMsg.NodeID)

		UpdateMemshipList(joinMsg)

		//Send full membershiplist to new join node
		joinAckMsg := MP.NewMessage(MP.JoinAckMsg, LocalID, MembershipList)
		joinAckPkg := MP.MsgToJSON(joinAckMsg)

		_, err := ln.WriteToUDP(joinAckPkg, joinAddr)
		if err != nil {
			log.Println(err.Error())
		}
		log.Printf("Introducer: JoinAck Sent to Node: %s...\n", joinMsg.NodeID)
	} else if joinMsg.MessageType == MP.LeaveMsg {
		log.Printf("Introducer: Introducer Leave... Close Port:%s...\n", MP.IntroducePort)
	}
}
