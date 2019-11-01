package sdfs

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"
	"sort"

	Config "../Config"
	Mem "../Membership"
)

var OpenNamenodeServer chan string = make(chan string)
var UpdateFilemapChan chan string = make(chan string)  //Receive failedNodeID

type FileMetadata struct {
	DatanodeList []string
	LastWrtTime  time.Time
}

type Namenode struct {
	Filemap map[string]*FileMetadata  //Key:sdfsFilename  Value:Pointer of metadata
	Nodemap map[string][]string 	  //Key:NodeID        Value:Pointer of fileList
}

//////////////////////////////////////////Functions////////////////////////////////////////////

func RunNamenodeServer() {

	<- OpenNamenodeServer

	var namenode = new(Namenode)

	namenode.Filemap = make(map[string]*FileMetadata)
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

	getCurrentMaps(namenode.Filemap, namenode.Nodemap)
	
	go WaitUpdateFilemapChan(namenode.Filemap, namenode.Nodemap)


	fmt.Printf("===RunNamenodeServer: Listen on port %s\n", Config.NamenodePort)
	err = http.Serve(listener, mux)
	if err != nil {
		log.Fatal("Serve(listener, nil) error: ", err)
	}
}

func WaitUpdateFilemapChan(Filemap map[string]*FileMetadata, Nodemap map[string][]string) {
	for {
		failedNodeID := <- UpdateFilemapChan

		//If failed nodeID can be found in Nodemap
		if reReplicaFileList, ok := Nodemap[failedNodeID]; ok {
			//delete from Nodemap
			delete(Nodemap, failedNodeID)

			//Also update Filemap and re-replicate files
			for _, filename := range reReplicaFileList {
				for idx, nodeID := range Filemap[filename].DatanodeList{
					if nodeID == failedNodeID {
						Filemap[filename].DatanodeList = append(Filemap[filename].DatanodeList[:idx], Filemap[filename].DatanodeList[idx+1:]...)
						break
					}
				}
				//TODO re-replicate the file
				fmt.Println("Start re-replicating...")
			}
		}
	}
}

///////////////////////////////////RPC Methods////////////////////////////
/*
	Given a request, return response containing a list of all Datanodes who has the file
*/

func (n *Namenode) GetDatanodeList(req FindRequest, resp *FindResponse) error {
	if filemetadata, ok := n.Filemap[req.Filename]; ok {
		resp.DatanodeList = filemetadata.DatanodeList
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

	datanodeList := append(Mem.GetListByRelateIndex(req.NodeID, []int{1,2,3}), req.NodeID)
	fmt.Println("datanodeList", datanodeList)

	//Updata Nodemap
	for _, datanodeID := range datanodeList {
		n.Nodemap[datanodeID] = append(n.Nodemap[datanodeID], req.Filename)
	}

	//Update Filemap
	filemetadata := FileMetadata{datanodeList, time.Now()}
	n.Filemap[req.Filename] = &filemetadata

	//Return
	resp.DatanodeList = datanodeList
	return nil
}

func (n *Namenode) DeleteFileMetadata(filename string, resp *bool) error {
	if filemetadata, ok := n.Filemap[filename]; ok {
		//Delete from Filemap
		delete(n.Filemap, filename)

		//Delete from Nodemap
		for _, datanodeID := range filemetadata.DatanodeList{
			for idx, storedfilename := range n.Nodemap[datanodeID]{
				if storedfilename == filename {
					n.Nodemap[datanodeID] = append(n.Nodemap[datanodeID][:idx], n.Nodemap[datanodeID][idx+1:]...)
					break
				}
			}
		}
		*resp = true
	} else {
		*resp = false
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
		n.Filemap[req.Filename].LastWrtTime = time.Now()
	} else {
		//Check if (curTime - lastWrtTime) > 60 s
		curTime := time.Now()
		lastWrtTime := n.Filemap[req.Filename].LastWrtTime
		timeDiff := curTime.Sub(lastWrtTime)

		if int64(timeDiff)-int64(60*time.Second) > 0 {
			*res = true
			n.Filemap[req.Filename].LastWrtTime = curTime
		} else {
			*res = false
		}
	}

	return nil
}

///////////////////////////////////Helper functions////////////////////////////

func insert(filemap map[string]*FileMetadata, sdfsfilename string, datanodeID string){
	if filemetadata, ok := filemap[sdfsfilename]; ok {
		//filemap[sdfsfilename] exist		
		filemetadata.DatanodeList = append(filemetadata.DatanodeList, datanodeID)
		sort.Strings(filemetadata.DatanodeList)
	} else {
		//filemap[sdfsfilename] not exist
		newfilemetadata := FileMetadata{[]string{datanodeID},time.Now()} //TODO Should LastWrtTime = time.Now()?
		filemap[sdfsfilename] = &newfilemetadata
	}
}

func checkReplica(sdfsfilename string, datanodelist []string) bool{
	n := len(datanodelist)

	if n > 3 {
		//At least 4 datanodes store the sdfsfile
		return true
	} else {
		//Not enough replicas
		//TODO re-replicate
		fmt.Println("Start re-replicating...")
		return false
	}
	
}


//TODO check this function
func getCurrentMaps(filemap map[string]*FileMetadata, nodemap map[string][]string) {
	//RPC datenodes to get FileList
	for _, nodeID := range Mem.MembershipList {
		nodeAddr := Config.GetIPAddressFromID(nodeID)

		client := NewClient(nodeAddr + ":" + Config.DatanodePort)
		client.Dial()
		
		var filelist []string
		client.rpcClient.Call("Datanode.GetFileList",Mem.LocalID, &filelist)
		
		nodemap[nodeID] = filelist

		client.Close()
	}

	//Figure out Filemap from Nodemap
	for datanodeID, fileList := range nodemap {
		for _, sdfsfilename := range fileList {
			insert(filemap, sdfsfilename, datanodeID)
		}
	}

	//Check if each sdfsfile has sufficient replicas
	for sdfsfilename, filemetadata := range filemap {
		checkReplica(sdfsfilename, filemetadata.DatanodeList)
	}
}
