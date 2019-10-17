package node

import (
	"log"
	"sort"
	"time"

	helper "../Helper"
)

var MembershipList []string
var MemHBMap map[string]time.Time = make(map[string]time.Time)
var MonitorList []string
var MayFailMap map[string]time.Time = make(map[string]time.Time)

type Updater struct{}

func UpdateMemshipList(recvMsg helper.Message) bool {
	msgType := recvMsg.MessageType
	senderID := recvMsg.NodeID
	contents := recvMsg.Content
	var updateOk bool
	switch msgType {
	case helper.JoinMsg:
		updateOk = AddNode(senderID)
	case helper.LeaveMsg:
		if contents[0] == LocalID {
			updateOk = true
		} else {
			updateOk = DeleteNode(contents[0])
		}
	case helper.FailMsg:
		updateOk = DeleteNode(contents[0])
	case helper.IntroduceMsg:
		updateOk = AddNode(contents[0])
	case helper.JoinAckMsg:
		updateOk = true
		MembershipList = contents
	case "Clear":
		MembershipList = []string{""}
		updateOk = true
	default:
		updateOk = false
	}
	if updateOk {
		updateMemHBMap()
		updateMonitorList()
	}
	return updateOk
}

func updateMonitorList() {
	MonitorList = helper.GetMonitorList(MembershipList, LocalAddress)
}

//Use MembershipList to update the key in MemHBMap(NodeID, Time)
func updateMemHBMap() {
	MemHBList := helper.GetMonitoringList(MembershipList, LocalAddress)
	if len(MemHBMap) == 0 {
		for _, c := range MemHBList {
			MemHBMap[c] = time.Now()
		}
	} else {
		var newMemHBMap map[string]time.Time = make(map[string]time.Time)
		for _, c := range MemHBList {
			if LastTime, ok := MemHBMap[c]; ok {
				newMemHBMap[c] = LastTime
			} else {
				newMemHBMap[c] = time.Now()
			}
		}
		MemHBMap = newMemHBMap
	}
}

func SortMembershipList() {
	sort.Strings(MembershipList)
}

func AddNode(newNodeID string) bool {
	log.Println("Updater: Before ADD Current List is: ")
	log.Print(MembershipList, "\n")
	_, found := FindNode(newNodeID)
	if !found {
		MembershipList = append(MembershipList, newNodeID)
		SortMembershipList()
		log.Print("Updater: New List is: ")
		log.Print(MembershipList, "\n")
		return true
	} else {
		return false
	}

}

func DeleteNode(nodeID string) bool {
	if nodeID == LocalID {
		return false
	}
	log.Println("Updater: Before Delete the List is: ")
	// fmt.Print(MembershipList, "\n")
	idx, found := FindNode(nodeID)
	log.Printf("The Delete Node is in the positition: %d\n", idx)
	if found {
		if idx != len(MembershipList)-1 {
			MembershipList = append(MembershipList[:idx], MembershipList[idx+1:]...)
		} else {
			MembershipList = MembershipList[:idx]
		}
		log.Print("Updater: New List is: ")
		log.Print(MembershipList, "\n")
		return true
	} else {
		return false
	}
}

func FindNode(nodeID string) (int, bool) {
	for i, c := range MembershipList {
		if c == nodeID {
			return i, true // return index
		}
	}
	return -1, false
}
