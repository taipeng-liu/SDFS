package membership

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"time"

	MP "../MsgProtocol"
	sdfs "../SDFS"
	Config "../Config"
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
		updateOk = addNode(senderID)
	case MP.LeaveMsg:
		if contents[0] == LocalID {
			updateOk = true
		} else {
			updateOk = deleteNode(contents[0])
		}
	case MP.FailMsg:
		updateOk = deleteNode(contents[0])
	case MP.IntroduceMsg:
		updateOk = addNode(contents[0])
	case MP.JoinAckMsg:
		updateOk = true
		MembershipList = contents
	case "Clear":
		MembershipList = []string{}
		updateOk = true
	default:
		updateOk = false
	}
	if updateOk {
		updateMemHBMap()
		updateMonitorList()
		//Todo: Should not go into this logic when there's no master
		if Config.IsMaster() {
			sdfs.UpdateNameNode(MembershipList)
		}
	}
	return updateOk
}

func WriteMemtableToJsonFile(fileAddr string) error {
	file, _ := json.MarshalIndent(MembershipList, "", " ")
	err := ioutil.WriteFile(fileAddr, file, 0644)
	return err
}

func ReadMemtableFromJsonFile(fileAddr string) ([]string, error) {
	jsonFile, err := os.Open(fileAddr)
	if err != nil {
		log.Println(err)
		return []string{}, err
	}

	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	var oldMemtable []string

	json.Unmarshal(byteValue, &oldMemtable)

	return oldMemtable, nil
}

/////////////////////////////////////////////////////////////////////////

func getListByRelateIndex(idxList []int) []string {
	var newList []string
	memListLen := len(MembershipList)

	if memListLen >= (len(idxList) + 1) {
		for i, nodeID := range MembershipList {
			if nodeID == LocalID {
				for _, idx := range idxList {
					newList = append(newList, MembershipList[(i+idx+memListLen)%memListLen])
				}
				break
			}
		}
	} else {
		for _, nodeID := range MembershipList {
			if nodeID != LocalID {
				newList = append(newList, nodeID)
			}
		}
	}
	return newList
}

func updateMonitorList() {
	MonitorList = getListByRelateIndex([]int{-1, 1, 2})
}

func updateMemHBMap() {
	MemHBList := getListByRelateIndex([]int{-2, -1, 1})
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

func addNode(newNodeID string) bool {
	log.Printf("addNode(): Adding nodeID %s...\n", newNodeID)
	_, found := findNode(newNodeID)
	if !found {
		log.Println("addNode(): Successfully added!")
		fmt.Printf("NodeID: %s join the group, welcome!\n", newNodeID)
		MembershipList = append(MembershipList, newNodeID)
		sort.Strings(MembershipList)
		log.Print("Updater: New List is: ")
		log.Print(MembershipList, "\n")
		return true
	} else {
		log.Println("addNode(): Add error: nodeID already exists")
		return false
	}

}

func deleteNode(nodeID string) bool {
	log.Printf("deleteNode(): Deleting nodeID %s...\n", nodeID)
	if nodeID == LocalID {
		log.Println("deleteNode(): Delete error: nodeID == LocalID")
		return false
	}
	idx, found := findNode(nodeID)
	if found {
		fmt.Printf("NodeID %s may fail or leave the group\n", nodeID)
		if idx != len(MembershipList)-1 {
			MembershipList = append(MembershipList[:idx], MembershipList[idx+1:]...)
		} else {
			MembershipList = MembershipList[:idx]
		}
		log.Println("deleteNode(): Successfully deleted!")
		return true
	} else {
		log.Println("deleteNode(): Delete error: nodeID not found")
		return false
	}
}

func findNode(nodeID string) (int, bool) {
	for i, c := range MembershipList {
		if c == nodeID {
			return i, true
		}
	}
	return -1, false
}
