package sdfs

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"reflect"
	"time"

	Config "../Config"
)

var namenode = new(Namenode)

type Metadata struct {
	DatanodeList []string
	LastWrtTime  time.Time
}

type Namenode struct {
	Filemap        map[string][]string //Key: sdfsFileName  Value: Arraylist of datanode
	Nodemap        map[string][]string //Key: NodeID  Value: Arraylist of sdfsFileName
	MembershipList []string
	Filetime       map[string]time.Time
}

//////////////////////////////////////////Functions////////////////////////////////////////////

func RunNamenodeServer() {

	namenode.Filemap = make(map[string][]string)
	namenode.Nodemap = make(map[string][]string)
	namenode.Filetime = make(map[string]time.Time)

	namenodeServer := rpc.NewServer()

	err := namenodeServer.Register(namenode)
	if err != nil {
		log.Fatal("Register(namenode) error:", err)
	}

	//======For multiple servers=====
	oldMux := http.DefaultServeMux
	mux := http.NewServeMux()
	http.DefaultServeMux = mux
	//===============================

	namenodeServer.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)

	//=======For multiple servers=====
	http.DefaultServeMux = oldMux
	//================================

	listener, err := net.Listen("tcp", ":"+Config.NamenodePort)
	if err != nil {
		log.Fatal("Listen error", err)
	}

	fmt.Printf("===RunNamenodeServer: Listen on port %s\n", Config.NamenodePort)
	err = http.Serve(listener, mux)
	if err != nil {
		log.Fatal("Serve(listener, nil) error: ", err)
	}

}

//***Todo: Check if it's correct
func UpdateNameNode(newMemList []string) {
	var addList, deleteList []string
	mapEq := reflect.DeepEqual(newMemList, namenode.MembershipList)
	if !mapEq {
		var newIdx, oldIdx int
		for newIdx, oldIdx = 0, 0; newIdx < len(newMemList) && oldIdx < len(namenode.MembershipList); {
			if newMemList[newIdx] == namenode.MembershipList[oldIdx] {
				newIdx++
				oldIdx++
			} else {
				//*** Todo: check validity
				if newMemList[newIdx] < namenode.MembershipList[oldIdx] {
					addList = append(addList, newMemList[newIdx])
					fmt.Printf("===New Added Node:%s\n", newMemList[newIdx])
					log.Printf("===New Added Node:%s\n", newMemList[newIdx])
					newIdx++
				} else {
					deleteList = append(deleteList, namenode.MembershipList[oldIdx])
					fmt.Printf("===Deleted Node:%s\n", namenode.MembershipList[oldIdx])
					log.Printf("===Deleted Node:%s\n", namenode.MembershipList[oldIdx])
					oldIdx++
				}
			}
		}
		for ; newIdx < len(newMemList); newIdx++ {
			addList = append(addList, newMemList[newIdx])
			fmt.Printf("===New Added Node:%s\n", newMemList[newIdx])
			log.Printf("===New Added Node:%s\n", newMemList[newIdx])
		}
		for ; oldIdx < len(namenode.MembershipList); oldIdx++ {
			deleteList = append(deleteList, namenode.MembershipList[oldIdx])
			fmt.Printf("===Deleted Node:%s\n", namenode.MembershipList[oldIdx])
			log.Printf("===Deleted Node:%s\n", namenode.MembershipList[oldIdx])
		}
	}

	namenode.MembershipList = newMemList
	fmt.Printf("namenode.MembershipList'size is %d!!\n", len(namenode.MembershipList))
	repFileSet := updateMap(addList, deleteList)
	reReplicate(repFileSet)
}

//***Todo: Update two essential maps
func updateMap(addList []string, deleteList []string) map[string]bool {
	//Set of sdfsfile to be re-replicated
	repFileSet := make(map[string]bool)
	// repFileSet := []string
	fmt.Printf("addList'size is %d!!\n", len(addList))
	fmt.Printf("deleteList'size is %d!!\n", len(deleteList))

	for _, nodeID := range deleteList {
		// fmt.Printf("Length of nodemap[%s] is: %d!!\n", nodeID, len(namenode.Nodemap[nodeID]))
		if len(namenode.Nodemap[nodeID]) == 0 {
			// fmt.Printf("Nothing to be delete for node %s\n!!", nodeID)
			continue
		}
		for _, fileName := range namenode.Nodemap[nodeID] {
			if ifExist, ok := repFileSet[fileName]; !ok && !ifExist {
				repFileSet[fileName] = true
				ifExist = true
				// fmt.Printf("What???? Find file %s in node %s??\n", fileName, nodeID)
			} else {
				log.Printf("file alreay exist in repFileSet!\n")
			}
		}
		delete(namenode.Nodemap, nodeID)
		fmt.Printf("updateMap: Remove nodeID: %s from NodeMap!!!\n", nodeID)
		log.Printf("updateMap: Remove nodeID: %s from NodeMap!!!\n", nodeID)
	}

	fmt.Printf("repFileSet'size is %d!!\n", len(repFileSet))
	for _, fileName := range repFileSet {
		fmt.Printf("file %s will be re-Replicate!!\n", fileName)
	}

	//Reassign replicas for this file
	for sdfsFileName := range repFileSet {
		for _, nodeID := range namenode.Filemap[sdfsFileName] {
			//***ToDo: Pick any correct node as LocalID
			if _, ok := namenode.Nodemap[nodeID]; ok {
				fmt.Printf("updateMap: Reassign nodeID: %s for sdfsfile: %s!!!\n", nodeID, sdfsFileName)
				log.Printf("updateMap: Reassign nodeID: %s for sdfsfile: %s!!!\n", nodeID, sdfsFileName)
				//New Replicas set for one sdfsFile
				namenode.Filemap[sdfsFileName] = Config.GetReplica(nodeID, namenode.MembershipList)

				//Namenode Caches all re-replicated files
				GetFile([]string{sdfsFileName, sdfsFileName}, false)

				//Add entry for new-add node list
				for _, val := range namenode.Filemap[sdfsFileName] {
					for _, addNodeID := range addList {
						if val == addNodeID {
							fmt.Printf("updateMap: Add entry for nodeID: %s for sdfsfile: %s!!!\n", addNodeID, sdfsFileName)
							log.Printf("updateMap: Add entry for nodeID: %s for sdfsfile: %s!!!\n", addNodeID, sdfsFileName)
							namenode.Nodemap[addNodeID] = append(namenode.Nodemap[addNodeID], sdfsFileName)
						}
					}
				}
				break
			}
		}
	}

	return repFileSet
}

//Todo: Rereplicate files for deleting Node
func reReplicate(repFileSet map[string]bool) {
	//Only re-replicate for each file once
	for sdfsFileName := range repFileSet {
		//***Replicate from sdfsfile?
		fmt.Printf("===Re-replicate file: %s!!!\n", sdfsFileName)
		//****Todo: Not namenode call!!!
		PutFile([]string{sdfsFileName, sdfsFileName}, false)
	}
	fmt.Printf("===Re-replicate returned!!\n")
	log.Printf("===Re-replicate returned!!\n")

}

///////////////////////////////////RPC Methods////////////////////////////
/*
	Given a request, return response containing a list of all Datanodes who has the file
*/

func (n *Namenode) GetDatanodeList(req FindRequest, resp *FindResponse) error {
	fmt.Printf("***namenode*** Enter GetDatanodeList! Filemap length is: %d!\n", len(n.Filemap))
	if _, ok := n.Filemap[req.Filename]; ok {
		resp.DatanodeList = n.Filemap[req.Filename]
	} else {
		resp.DatanodeList = []string{}
	}
	return nil
}

/*
	First time for put original file (Assign to Mmonitoring List AKA MemHBList)
	Insert pair (sdfsfilename, datanodeList) into Filemap
	Send datanodeList back to InsertResponse
*/
func (n *Namenode) InsertFile(req InsertRequest, resp *InsertResponse) error {

	datanodeList := Config.GetReplica(req.Hostname, namenode.MembershipList)
	fmt.Println("GetReplica succeed! datanodeList'size is: %d!!\n", len(datanodeList))
	log.Println("GetReplica succeed! datanodeList'size is: %d!!\n", len(datanodeList))

	for _, datanodeID := range datanodeList {
		fmt.Printf("**namenode**: Insert sdfsfile: %s to %s from %s\n", req.Filename, datanodeID, req.Hostname)
		log.Printf("**namenode**: Insert sdfsfile: %s to %s from %s\n", req.Filename, datanodeID, req.Hostname)
		n.Filemap[req.Filename] = append(n.Filemap[req.Filename], datanodeID)
		n.Nodemap[datanodeID] = append(n.Nodemap[datanodeID], req.Filename)
		n.Filetime[req.Filename] = time.Now()
	}
	// n.Filemap[InsertRequest.Filename] = datanodeList

	resp.DatanodeList = datanodeList
	return nil
}

func (n *Namenode) DeleteFile(req DeleteRequest, resp *DeleteResponse) error {

	var findFlag bool = false
	delete(n.Filemap, req.Filename)
	for nodeID, nodeFile := range n.Nodemap {
		for idx, fileName := range nodeFile {
			if req.Filename == fileName {
				fmt.Printf("Delete Entry for File %s in %s!!\n", fileName, nodeID)
				nodeFile = append(nodeFile[:idx], nodeFile[idx+1:]...)
				fmt.Printf("NodeMap for nodeID %s is: %d!!\n", nodeID, len(nodeFile))
				findFlag = true
				break
			}
		}
		n.Nodemap[nodeID] = nodeFile
	}
	if !findFlag {
		resp.Statement = "No such File??"
	}
	return nil
}

/*
	First check if the client MUST write to file. If not, check Filemap
	and calculate time difference to dicide whether give write permission.
*/
func (n *Namenode) GetWritePermission(req PermissionRequest, res *bool) error {
	if req.MustWrite {
		*res = true
		n.Filetime[req.Filename] = time.Now()
	} else {
		//Check if (curTime - lastWrtTime) > 60 s
		curTime := time.Now()
		lastWrtTime := n.Filetime[req.Filename]
		timeDiff := curTime.Sub(lastWrtTime)

		if int64(timeDiff)-int64(60*time.Second) > 0 {
			*res = true
			n.Filetime[req.Filename] = curTime
		} else {
			*res = false
		}
	}

	return nil
}
