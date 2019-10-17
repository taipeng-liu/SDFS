package membership

import (
	"fmt"

	MP "../MsgProtocol"
	Config "../Config"
)

var curNode *Node = CreateNewNode()

var KillHBListener chan struct{} = make(chan struct{})
var KillHBSender chan struct{} = make(chan struct{})
var KillMsgListener chan struct{} = make(chan struct{})
var KillIntroducer chan struct{} = make(chan struct{})
var KillHBTimer chan struct{} = make(chan struct{})

var LocalAddress string
var LocalID string
var Status bool

type Node struct {
	Sender
	Listener
	Introducer
}

func CreateNewNode() *Node {
	var newNode *Node = new(Node)
	return newNode
}

//Called from main.go when the command is "Join\n"
//Create new node and run Listener,Sender and Updater
//in seperate goroutines
func RunNode(isIntroducer bool) {
	LocalID = Config.CreateID()
	fmt.Println("Node: Local ID is: " + LocalID)
	LocalAddress = Config.GetHostName()
	fmt.Println("Node: Local Address is: " + LocalAddress)
	Status = true

	go curNode.Listener.RunMSGListener()
	if !isIntroducer {
		//Non-intro send JoinMsg to Introducer
		curNode.Sender.SendJoin()
	} else {
		//Introducer receive JoinMsg from non-intro
		go curNode.Introducer.NodeHandleJoin()
	}

	go curNode.Listener.RunHBListener()
	go curNode.Sender.SendHeartbeat()
}

//Called from main.go when the command is "Leave\n"
func StopNode() {
	curNode.Sender.SendLeave()
	KillMsgListener <- struct{}{}
	KillHBSender <- struct{}{}

	if Config.IsIntroducer() {
		KillIntroducer <- struct{}{}
	}
	Status = false

	//When Leave, Clear all elements
	UpdateMemshipList(MP.Message{"Clear","",[]string{""}})

	fmt.Println("Node: Stop Node...")
}

//Called from main.go when the command is "List\n"
func ShowList() {
	if Status {
		fmt.Println("The current membership list is:")
		for _, str := range MembershipList {
			fmt.Println(str)
		}
	} else {
		fmt.Println("This server doesn't belong to a group")
	}
	return
}

//Called from main.go when the command is "ID\n"
func ShowID() {
	if Status {
		fmt.Println("The current node ID is:")
		fmt.Print(LocalID, "\n")
	} else {
		fmt.Println("This server doesn't belong to a group")
	}
}
