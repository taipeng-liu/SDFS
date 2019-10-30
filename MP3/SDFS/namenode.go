package sdfs

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"reflect"

	Config "../Config"
)

var namenode = new(Namenode)

type Namenode struct {
	Filemap        map[string][]string //Key: sdfsFileName  Value: Arraylist of replica node
	Nodemap        map[string][]string //Key: NodeID  Value: Arraylist of sdfsFileName
	MembershipList []string
}

//////////////////////////////////////////Functions////////////////////////////////////////////

func RunNamenodeServer() {

	namenode.Filemap = make(map[string][]string)
	namenode.Nodemap = make(map[string][]string)

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
	fmt.Printf("addList'size is %d!!\n", len(addList))
	fmt.Printf("deleteList'size is %d!!\n", len(deleteList))

	for _, nodeID := range deleteList {
		for _, fileName := range namenode.Nodemap[nodeID] {
			if _, ok := repFileSet[fileName]; !ok {
				repFileSet[fileName] = true
			}
		}
		delete(namenode.Nodemap, nodeID)
		fmt.Printf("updateMap: Remove nodeID: %s from NodeMap!!!\n", nodeID)
		log.Printf("updateMap: Remove nodeID: %s from NodeMap!!!\n", nodeID)
	}

	fmt.Printf("repFileSet'size is %d!!\n", len(repFileSet))

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

/*TODO Implement GetDatanodeList
func (n *Namenode) GetDatanodeList(req FindRequest, resp *FindResponse) error {
	resp.DatanodeList = []string{"fa19-cs425-g73-01.cs.illinois.edu",
				     "fa19-cs425-g73-02.cs.illinois.edu",
				     "fa19-cs425-g73-03.cs.illinois.edu"}
	return nil
}

TODO Implement InsertFile
func (n *Namenode) InsertFile(req InsertRequest, resp *InsertResponse) error {
	resp.DatanodeList = []string{"fa19-cs425-g73-01.cs.illinois.edu",
				     "fa19-cs425-g73-02.cs.illinois.edu",
				     "fa19-cs425-g73-03.cs.illinois.edu"}
	return nil
}
*/
func (n *Namenode) GetDatanodeList(req *FindRequest, resp *FindResponse) error {
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

	datanodeList := Config.GetReplica(req.LocalID, namenode.MembershipList)
	fmt.Println("GetReplica succeed! datanodeList'size is: %d!!\n", len(datanodeList))
	log.Println("GetReplica succeed! datanodeList'size is: %d!!\n", len(datanodeList))

	for _, datanodeID := range datanodeList {
		fmt.Printf("**namenode**: Insert sdfsfile: %s to %s from %s\n", req.Filename, datanodeID, req.LocalID)
		log.Printf("**namenode**: Insert sdfsfile: %s to %s from %s\n", req.Filename, datanodeID, req.LocalID)
		n.Filemap[req.Filename] = append(n.Filemap[req.Filename], datanodeID)
		n.Nodemap[datanodeID] = append(n.Nodemap[datanodeID], req.Filename)
	}
	// n.Filemap[InsertRequest.Filename] = datanodeList

	resp.DatanodeList = datanodeList
	return nil
}

//TODO
//Note: Map operation is not required to be implemented.
//If we do, please implement them into FUNCTION, NOT METHOD.
//The reason is that class Namenode is registered in RPC.
//All methods of Namenode MUST have a standard format like
//func (a Type) method([Valuable of Explicit Type], [Pointer of Explicit Type]) error{}

/*
func (n *Namenode) Add(nodeID string, sdfsfilename string) {
	return
}

func (n *Namenode) Delete() {
	//TODO
	//delete an item from filemap by key
	//return deleted key and value
	return
}

func (n *Namenode) Find() {
	//TODO
	//find value by key
	//return value if found or nil
	return
}

func (n *Namenode) Update() {
	//TODO
	//modify value by key
	//return modified key and value
	return
}


*/
