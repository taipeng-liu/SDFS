package node

import (
	"fmt"
	"log"
	"net"
	"time"

	msg "../Helper"
)

type Listener struct {
}



func BuildUDPServer(ConnPort string) *net.UDPConn {
	udpAddr, err := net.ResolveUDPAddr(msg.ConnType, ":"+ConnPort)
	if err != nil {
		log.Fatal(err)
	}

	ln, err := net.ListenUDP(msg.ConnType, udpAddr)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("===BuildUDPServer: Listen on Port" + ConnPort)

	return ln
}

func (l *Listener) RunMSGListener() {
	ln := BuildUDPServer(msg.ConnPort)

	for {
		select {
		case <-KillMsgListener:
			ln.Close()
			fmt.Println("===Listener: MSGListener Leave!!")
			// KillRoutine <- struct{}{}
			return
		default:
			HandleListenMsg(ln)
		}
	}
}

func HandleListenMsg(conn *net.UDPConn) {

	msgBuf := make([]byte, 1024)

	n, _, err := conn.ReadFromUDP(msgBuf)
	if err != nil {
		log.Fatal(err)
	}
	receivedMsg := msg.JSONToMsg([]byte(string(msgBuf[:n])))
	msgType := receivedMsg.MessageType
	senderID := receivedMsg.NodeID
	contents := receivedMsg.Content
	log.Printf("Listener: Recieve %s message from Node: %s", msgType, senderID)

	switch receivedMsg.MessageType {
	case msg.LeaveMsg:
		updateOk := UpdateMemshipList(receivedMsg)
		if updateOk {
			log.Printf("Listener: NodeID %s is recognized as leave...\n", contents[0])
			SendLeaveMsg(conn, senderID, contents[0])
		}
	case msg.IntroduceMsg:
		updateOk := UpdateMemshipList(receivedMsg)	
		if updateOk {
			log.Printf("Listener: NodeID %s join the group, welcome!\n", contents[0])
			SendIntroduceMsg(conn, senderID, contents[0])
		}
	case msg.FailMsg:
		if contents[0] != LocalID {
			updateOk := UpdateMemshipList(receivedMsg)
			if updateOk {
				log.Printf("Listener: NodeID %s is detected as fail...\n", contents[0])
				SendFailMsg(conn, senderID, contents[0])
			}
		} else {
			// fmt.Printf("Fail Msg: I'm gonna Delete myself sent from %s !!\n", receivedMsg.NodeID)
			// if SelfFailTime%3 == 0 {
			// 	joinSucceed := SendJoinMsg(msg.IntroducerAddress)
			// 	if !joinSucceed {
			// 		fmt.Println("Introducer is down!!")
			// 		return
			// 	}
			// }
			// SelfFailTime += 1
			/***********************************
			time.Sleep(3 * time.Second)
			joinSucceed := SendJoinMsg(msg.IntroducerAddress)
			if !joinSucceed {
				fmt.Println("Introducer is down!!")
				return
			}
			fmt.Printf("Fail Msg: I'm gonna Delete myself sent from %s !!\n", receivedMsg.NodeID)

			// StopNode()
			***********************************/
			go StopNode()
			time.Sleep(3*time.Second)
			go RunNode(msg.IsIntroducer())
		/******************************************
		} else if FindNode(MembershipList, receivedMsg.NodeID) != -1 {
			// fmt.Println("Fail Msg: Delete Node!!")
			ret := FindNode(MembershipList, receivedMsg.Content[0])
			if ret != -1 {
				//I have a update on MemList, so this is the first time I receive the msg
				//and I will send to other nodes this new msg!!!!!
				log.Printf("Listener: NodeID %s is recognized as failed...\n", receivedMsg.Content[0])
				SendFailMsg(conn, receivedMsg.NodeID, receivedMsg.Content[0])
				_ = DeleteNode(receivedMsg.Content[0])
				UpdateMemHBMap()
			}
		*********************************************/
		}
	default:
		fmt.Println("===Listener:Can't recognize the msg")
	}
	log.Println("Listener: Return from HandleListenMsg ")
}

//Counting the timeout
func HBTimer(ln *net.UDPConn) {
	for {
		select {
		case <-KillHBTimer:
			// ln.Close()
			fmt.Println("===Listener: Timer Leave!!")
			// KillRoutine <- struct{}{}
			return
		default:
			time.Sleep(2 * time.Second)
			curTime := time.Now()
			for NodeID, lastTime := range MemHBMap {
				timeDiff := curTime.Sub(lastTime)
				// fmt.Printf("===HBTimer: For %d duration not received message from %s!!===\n", int64(timeDiff), NodeID)
				// log.Printf("===HBTimer: For %d duration not received message from %s!!===\n", int64(timeDiff), NodeID)
				_, ok := MayFailMap[NodeID]
				if ok {
					if int64(timeDiff)-msg.TimeOut*int64(time.Millisecond) > 0 {
						updateOk := UpdateMemshipList(msg.Message{msg.FailMsg, LocalID,[]string{NodeID}})
						if updateOk {
							//I have a update on MemList, so this is the first time I receive the msg
							//and I will send to other nodes this new msg!!!!!
							log.Printf("HBTimer: %s timeout!! timeDiff is %s\n", NodeID, timeDiff.String())
							SendFailMsg(ln, "", NodeID)
						}
					}
					delete(MayFailMap, NodeID)
				} else {
					if int64(timeDiff)-msg.TimeOut*int64(time.Millisecond) > 0 {
						//You may fail...
						MayFailMap[NodeID] = time.Now()
					}
				}
			}
		}
	}
}

//Listen to Heartbeat and Check timeout
func (l *Listener) RunHBListener() {

	ln := BuildUDPServer(msg.HeartbeatPort)

	hbBuf := make([]byte, 2048)

	go HBTimer(ln)
	for {
		// select {
		// case <-KillHBListener:
		// 	KillHBTimer <- struct{}{}
		// 	ln.Close()
		// 	fmt.Println("===Listener: HBListener Leave!!")
		// 	return
		// default:
		n, _, err := ln.ReadFromUDP(hbBuf)
		if err != nil {
			log.Println(err)
		}

		receivedMsg := msg.JSONToMsg([]byte(string(hbBuf[:n])))

		log.Printf("HBListener: Received Message Type: %s from %s...\n", receivedMsg.MessageType, receivedMsg.NodeID)

		if receivedMsg.MessageType == msg.HeartbeatMsg {
			if _, ok := MemHBMap[receivedMsg.NodeID]; ok {
				MemHBMap[receivedMsg.NodeID] = time.Now()
			} else {
				//log.Println("Listener: MemHBMap doesn't contain the NodeID" + receivedMsg.NodeID)
			}
			continue
		} else if receivedMsg.MessageType == msg.LeaveMsg && receivedMsg.NodeID == LocalID {
			KillHBTimer <- struct{}{}
			ln.Close()
			fmt.Println("===Listener: HBListener Leave!!")
			return
			// log.Println("Listener:Recieve Heartbeat from NodeID:", receivedMsg.NodeID)
		}
	}
}
