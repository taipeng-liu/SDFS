package membership

import (
	"log"
	"sort"
	"time"
	"fmt"

	MP "../MsgProtocol"
)

var MembershipList []string
var MemHBMap map[string]time.Time = make(map[string]time.Time)
var MonitorList []string
var MayFailMap map[string]time.Time = make(map[string]time.Time)

func UpdateMemshipList(recvMsg MP.Message) bool {
	msgType := recvMsg.MessageType
	senderID := recvMsg.NodeID
	contents := recvMsg.Content
	var updateOk bool
	switch msgType {
	case MP.JoinMsg:
		updateOk = AddNode(senderID)
	case MP.LeaveMsg:
		if contents[0] == LocalID {
			updateOk = true
		} else {
			updateOk = DeleteNode(contents[0])
		}
	case MP.FailMsg:
		updateOk = DeleteNode(contents[0])
	case MP.IntroduceMsg:
		updateOk = AddNode(contents[0])
	case MP.JoinAckMsg:
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

func getListByRelateIndex(idxList []int) []string{
	var newList []string
	memListLen := len(MembershipList)
	
	if memListLen >= (len(idxList) + 1) {
		for i, nodeID := range MembershipList {
			if nodeID == LocalID{
				for _, idx := range idxList {
					newList = append(newList, MembershipList[(i+idx+memListLen)%memListLen])
				}
				break
			}
		}
	} else {
		for _, nodeID := range MembershipList {
			if nodeID != LocalID{
				newList = append(newList, nodeID)
			}
		}
	}
	return newList
}

func updateMonitorList() {
	MonitorList = getListByRelateIndex([]int{-1,1,2})
}

func updateMemHBMap() {
	MemHBList := getListByRelateIndex([]int{-2,-1,1})
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

func AddNode(newNodeID string) bool {
	log.Printf("AddNode(): Adding nodeID %s...\n",newNodeID)
	_, found := FindNode(newNodeID)
	if !found {
		log.Println("AddNode(): Successfully added!")
		fmt.Printf("NodeID: %s join the group, welcome!\n", newNodeID)
		MembershipList = append(MembershipList, newNodeID)
		sort.Strings(MembershipList)
		log.Print("Updater: New List is: ")
		log.Print(MembershipList, "\n")
		return true
	} else {
		log.Println("AddNode(): Add error: nodeID already exists")
		return false
	}

}

func DeleteNode(nodeID string) bool {
	log.Printf("DeleteNode(): Deleting nodeID %s...\n", nodeID)
	if nodeID == LocalID {
		log.Println("DeleteNode(): Delete error: nodeID == LocalID")
		return false
	}
	idx, found := FindNode(nodeID)
	if found {
		fmt.Printf("NodeID %s may fail or leave the group\n", nodeID)
		if idx != len(MembershipList)-1 {
			MembershipList = append(MembershipList[:idx], MembershipList[idx+1:]...)
		} else {
			MembershipList = MembershipList[:idx]
		}
		log.Println("DeleteNode(): Successfully deleted!")
		return true
	} else {
		log.Println("DeleteNode(): Delete error: nodeID not found")
		return false
	}
}

func FindNode(nodeID string) (int, bool) {
	for i, c := range MembershipList {
		if c == nodeID {
			return i, true
		}
	}
	return -1, false
}
