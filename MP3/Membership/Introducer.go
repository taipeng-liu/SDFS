package membership

import (
	"fmt"
	"log"
	"net"

	MP "../MsgProtocol"
	Conn "../Connection"
	Config "../Config"
)

type Introducer struct{}

func (i *Introducer) NodeHandleJoin() {
	//Get old membertable up to last leave/fail
	Memtable, err := ReadMemtableFromJsonFile("Config/Memtable.json")
	if err!= nil {
		log.Fatal("Unable to get old Memtable", err)
	}

	//Create a rejoin msg
	rejoinMsg := MP.NewMessage(MP.IntroducerRejoinMsg, LocalID, []string{""})	
	rejoinPkg := MP.MsgToJSON(rejoinMsg)
	var oldGroupExist bool = false

	//Try to rejoin by iterating oldMemtable
	for _, oldMemberID := range Memtable {

		oldMemberAddr := Config.GetIPAddressFromID(oldMemberID)
		if oldMemberAddr == LocalAddress {
			continue
		}

		conn := Conn.BuildUDPClient(oldMemberAddr, Config.ConnPort)
		defer conn.Close()

		Conn.WriteToUDPConn(rejoinPkg, conn)

		n, joinAck := Conn.ReadUDP(conn)
		if n == -1 {
			fmt.Printf("OldMember %s is down, try next one...\n", oldMemberID)
			continue
		} else {
			joinAckMsg := MP.JSONToMsg([]byte(string(joinAck[:n])))
			if joinAckMsg.MessageType == MP.JoinAckMsg{
				oldGroupExist = true
				UpdateMemshipList(joinAckMsg)
				fmt.Println("Found old group!")
			}
			break
		}
	}

	

	//Add Introducer itself to MemList
	ok := UpdateMemshipList(MP.Message{MP.JoinMsg,LocalID,[]string{""}})
	if !ok {
		log.Fatal("Unable to add Introducer itself to Memtable", err)
		return
	}
	
	err = WriteMemtableToJsonFile("Config/Memtable.json")
	if err != nil {
		log.Println("Writing to JsonFile is unable")
	}

	ln := Conn.BuildUDPServer(Config.IntroducePort)

	if oldGroupExist {
		SendIntroduceMsg(ln, "", LocalID)
	}

	//Handle JoinMsg
	var stop bool = false
	for {
		if stop {
			ln.Close()
			fmt.Println("====Introducer: Leave!!")
			return
		} else {
			fmt.Println("====Introducer: Waiting for new join...")
			HandleJoinMsg(ln, &stop)
		}
	}

}

func HandleJoinMsg(ln *net.UDPConn, stop *bool) {
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
		WriteMemtableToJsonFile("Config/Memtable.json")

		//Send full membershiplist to new join node
		joinAckMsg := MP.NewMessage(MP.JoinAckMsg, LocalID, MembershipList)
		joinAckPkg := MP.MsgToJSON(joinAckMsg)

		_, err := ln.WriteToUDP(joinAckPkg, joinAddr)
		if err != nil {
			log.Println(err.Error())
		}
		log.Printf("Introducer: JoinAck Sent to Node: %s...\n", joinMsg.NodeID)
	} else if joinMsg.MessageType == MP.LeaveMsg {
		*stop = true
		log.Printf("Introducer: Introducer Leave... Close Port:%s...\n", Config.IntroducePort)
	}
}
