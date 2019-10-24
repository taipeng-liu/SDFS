package membership

import (
	"fmt"
	"log"
	"net"
	"time"
	Config "../Config"
	MP "../MsgProtocol"
	Conn "../Connection"
)

type Listener struct {
}

func (l *Listener) RunMSGListener() {
	ln := Conn.BuildUDPServer(Config.ConnPort)

	for {
		select {
		case <-KillMsgListener:
			ln.Close()
			fmt.Println("===Listener: MSGListener Leave!!")
			return
		default:
			HandleListenMsg(ln)
		}
	}
}

func HandleListenMsg(conn *net.UDPConn) {

	msgBuf := make([]byte, 1024)

	n, senderAddr, err := conn.ReadFromUDP(msgBuf)
	if err != nil {
		log.Fatal(err)
	}
	receivedMsg := MP.JSONToMsg([]byte(string(msgBuf[:n])))
	msgType := receivedMsg.MessageType
	senderID := receivedMsg.NodeID
	contents := receivedMsg.Content
	log.Printf("Listener: Recieve %s message from Node: %s", msgType, senderID)

	switch receivedMsg.MessageType {
	case MP.LeaveMsg:
		updateOk := UpdateMemshipList(receivedMsg)
		if updateOk {
			log.Printf("Listener: NodeID %s is recognized as leave...\n", contents[0])
			SendLeaveMsg(conn, senderID, contents[0])
			if Config.IsIntroducer() {
				WriteMemtableToJsonFile("Config/Memtable.json")
			}
		}
	case MP.IntroduceMsg:
		updateOk := UpdateMemshipList(receivedMsg)	
		if updateOk {
			log.Printf("Listener: NodeID %s join the group, welcome!\n", contents[0])
			SendIntroduceMsg(conn, senderID, contents[0])
		}
	case MP.FailMsg:
		if contents[0] != LocalID {
			updateOk := UpdateMemshipList(receivedMsg)
			if updateOk {
				log.Printf("Listener: NodeID %s is detected as fail...\n", contents[0])
				SendFailMsg(conn, senderID, contents[0])
				if Config.IsIntroducer() {
					WriteMemtableToJsonFile("Config/Memtable.json")
				}
			}
		} else {
			go StopNode()
			time.Sleep(3*time.Second)
			go RunNode(Config.IsIntroducer())
		}
	case MP.IntroducerRejoinMsg:
		joinAckMsg := MP.NewMessage(MP.JoinAckMsg, LocalID, MembershipList)
		joinAckPkg := MP.MsgToJSON(joinAckMsg)
		
		conn.WriteToUDP(joinAckPkg, senderAddr)
	default:
		fmt.Println("===Listener:Can't recognize the msg")
	}
	log.Println("Listener: Return from HandleListenMsg ")
}

func HBTimer(ln *net.UDPConn) {
	for {
		select {
		case <-KillHBTimer:
			fmt.Println("===Listener: Timer Leave!!")
			return
		default:
			time.Sleep(2 * time.Second)
			curTime := time.Now()
			for NodeID, lastTime := range MemHBMap {
				timeDiff := curTime.Sub(lastTime)
				_, ok := MayFailMap[NodeID]
				if ok {
					if int64(timeDiff)-Config.TimeOut*int64(time.Millisecond) > 0 {
						updateOk := UpdateMemshipList(MP.Message{MP.FailMsg, LocalID,[]string{NodeID}})
						if updateOk {
							log.Printf("HBTimer: %s timeout!! timeDiff is %s\n", NodeID, timeDiff.String())
							SendFailMsg(ln, "", NodeID)
							if Config.IsIntroducer() {
								WriteMemtableToJsonFile("Config/Memtable.json")
							}
						}
					}
					delete(MayFailMap, NodeID)
				} else {
					if int64(timeDiff)-Config.TimeOut*int64(time.Millisecond) > 0 {
						MayFailMap[NodeID] = time.Now()
					}
				}
			}
		}
	}
}

func (l *Listener) RunHBListener() {

	ln := Conn.BuildUDPServer(Config.HeartbeatPort)
	hbBuf := make([]byte, 2048)
	go HBTimer(ln)
	for {
		n, _, err := ln.ReadFromUDP(hbBuf)
		if err != nil {
			log.Println(err)
		}

		receivedMsg := MP.JSONToMsg([]byte(string(hbBuf[:n])))

		log.Printf("HBListener: Received Message Type: %s from %s...\n", receivedMsg.MessageType, receivedMsg.NodeID)

		if receivedMsg.MessageType == MP.HeartbeatMsg {
			if _, ok := MemHBMap[receivedMsg.NodeID]; ok {
				MemHBMap[receivedMsg.NodeID] = time.Now()
			}
			continue
		} else if receivedMsg.MessageType == MP.LeaveMsg && receivedMsg.NodeID == LocalID {
			KillHBTimer <- struct{}{}
			ln.Close()
			fmt.Println("===Listener: HBListener Leave!!")
			return
		}
	}
}
