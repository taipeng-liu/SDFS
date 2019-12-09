package sdfs

import (
	"errors"
	"fmt"
	//"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	//"os"
	"sort"
	"strings"
	"time"

	Config "../Config"
	Mem "../Membership"
)

var OpenNamenodeServer chan string = make(chan string)
var UpdateFilemapChan chan string = make(chan string) //Receive failedNodeID
var TaskChan chan *Task = make(chan *Task)
var TaskKeeperChan chan *Task = make(chan *Task)
var deleteFilesRequest chan bool = make(chan bool)
var cachemap map[string][]string
var WorkerWhoFinishTask chan string = make(chan string)

type FileMetadata struct {
	DatanodeList []string
	LastWrtTime  time.Time
}

type Namenode struct {
	Filemap    map[string]*FileMetadata //Key:sdfsFilename  Value:Pointer of metadata
	Nodemap    map[string][]string      //Key:NodeID        Value:Pointer of fileList
	Workingmap map[string]*WorkerInfo   //Key:NodeID        Value:Pointer of Task
}


///////////////////////////////////RPC Methods////////////////////////////
func (n *Namenode) RunMapper(mapperArg MapperArg, res *int) error {
	mapper := mapperArg.Maple_exe
	N := mapperArg.Num_maples
	prefix := mapperArg.Sdfs_intermediate_filename_prefix
	src_dir := mapperArg.Sdfs_src_directory

	//Find all sdfs_files which come from src_dir, return a list of filename
	fileList := findFileWithPrefix(src_dir+"/", n.Filemap)
	if len(fileList) == 0 {
		*res = 0
		return errors.New("Namenode.RunMapper: cannot find files")
	}

	//Split fileList into taskList, return a list of Task
	taskList := rangePartition(fileList, N, "map", mapper, prefix, nil)

	//taskKeeper, keep tracing each task and deal with node failure
	go taskKeeper(N, n.Workingmap, "map", false)

	//Evoke all nodes
	for NodeID, _ := range n.Workingmap {
		if NodeID == Mem.LocalID {
			continue
		}
		go waitForTaskChan(NodeID, n.Workingmap)
	}

	go distributeAllTasks(taskList)

	*res = 1
	return nil
}

func (n *Namenode) RunReducer(reducerArg ReducerArg, res *int) error {
	reducer := reducerArg.Juice_exe
	N := reducerArg.Num_juices
	//prefix := reducerArg.Sdfs_intermediate_filename_prefix
	destfilename := reducerArg.Sdfs_dest_filename
	delete_input := reducerArg.Delete_input
	partition_way := reducerArg.Partition_way

	//Find all sdfs_files with the prefix, returns a list of filename
	cacheMap := cachemap
	if len(cacheMap) == 0 {
		*res = 0
		return errors.New("Namenode.RunMapper: cannot find files")
	}

	fileList := getFileListFromCacheMap(cacheMap)

	var taskList []*Task
	if partition_way == "hash" || strings.Contains(partition_way, "hash") {
		taskList = hashPartition(fileList, N, "reduce", reducer, destfilename, cacheMap)
	} else if partition_way == "range" || strings.Contains(partition_way, "range") {
		taskList = rangePartition(fileList, N, "reduce", reducer, destfilename, cacheMap)
	} else {
		fmt.Println("Invalid partition way: only support hash or range partition")
		return nil
	}

	go deleteInputFiles(n.Workingmap)

	go managePrivateChan(n.Workingmap)

	//taksKeeper
	go taskKeeper(N, n.Workingmap, "reduce", delete_input)

	//Evoke all nodes
	for NodeID, _ := range n.Workingmap {
		if NodeID == Mem.LocalID {
			continue
		}

		go waitForTaskChan(NodeID, n.Workingmap)
	}

	go distributeAllTasks(taskList)

	*res = 1
	return nil
}

func (n *Namenode) SendWorkerFinishMsg(nodeID string, res *int) error {
	go sendToPrivateChanManager(nodeID)
	return nil
}

//////////////////////////////////Goroutine//////////////////////////////////////////////
func sendToPrivateChanManager(nodeID string) {
	WorkerWhoFinishTask <- nodeID
}

func managePrivateChan(Workingmap map[string]*WorkerInfo) {
	for {
		nodeID := <- WorkerWhoFinishTask
		Workingmap[nodeID].PrivateChan <- "finished"
	}
}

func deleteInputFiles(Workingmap map[string]*WorkerInfo) {
	delete_input := <-deleteFilesRequest

	if delete_input {
		for nodeID, _ := range Workingmap {
			//RPC node to delete all intermediate files
			nodeAddr := Config.GetIPAddressFromID(nodeID)

			client := NewClient(nodeAddr + ":" + Config.DatanodePort)
			client.Dial()

			if err := client.Delete("cache"); err != nil {
				log.Println("Namenode.deleteInputFiles.client.Delete: error at node ", nodeID)
			}

			client.Close()
		}
	fmt.Println("All Cache Cleared")
	}
}

func distributeAllTasks(taskList []*Task) {
	for _, taskPointer := range taskList {
		TaskChan <- taskPointer
	}
}

func waitForTaskChan(NodeID string, Workingmap map[string]*WorkerInfo) {
	for {
		task := <-TaskChan

		if task != nil {
			//update worker information in Workingmap
			Workingmap[NodeID].TaskList = append(Workingmap[NodeID].TaskList, task)

			//Rpc datanode to work
			nodeAddr := Config.GetIPAddressFromID(NodeID)
			client := NewClient(nodeAddr + ":" + Config.DatanodePort)
			client.Dial()

			var res int
			if err := client.rpcClient.Call("Datanode.RunMapReduce", *task, &res); err != nil {
				log.Println(err)
			}

			client.Close()

			//wait
			<-Workingmap[NodeID].PrivateChan

			fmt.Println("Namenode: Datanode.RunMapReduce() returns, remainTask--")
			//When a task is finished, send nil to TaskKeeperChan
			TaskKeeperChan <- nil

		} else {
			//receive nil, return
			return
		}
	}
}

//Check all tasks are done
//If a node fail, give the task to another node
func taskKeeper(remainTask int, Workingmap map[string]*WorkerInfo, taskType string, delete_input bool) {
	defer Config.TimeCount()()
	for {
		NilorTask := <-TaskKeeperChan

		if NilorTask != nil {
			TaskChan <- NilorTask
		} else {
			remainTask--

			if remainTask == 0 {
				fmt.Println("TaskKeeper: remainTask is zero")

				for i := 0; i < len(Workingmap); i++ {
					TaskChan <- nil
				}

				//Request submission
				for nodeID,_ := range Workingmap {
					requestTaskSubmission(nodeID, taskType, Workingmap)
				}



				switch taskType {
				case "reduce":
					if delete_input {
						deleteFilesRequest <- true
					} else {
						deleteFilesRequest <- false
					}
				case "map":
					cachemap = getCacheMapFromWorkingmap(Workingmap)
				}

				fmt.Printf("====TaskKeeper: All %s tasks finished!\n", taskType)

				return
			}
		}
	}
}

//RPC nodeID to submit a job
func requestTaskSubmission(nodeID string, taskType string, Workingmap map[string]*WorkerInfo) {
	fmt.Println("Namenode.RequestTaskSubmission:", nodeID)
	nodeAddr := Config.GetIPAddressFromID(nodeID)

	client := NewClient(nodeAddr + ":" + Config.DatanodePort)
	client.Dial()

	var intermediateFile []string
	if err := client.rpcClient.Call("Datanode.SubmitTask", taskType, &intermediateFile); err != nil {
		fmt.Println("Namenode.requestTaskSubmission().client.rpcClient.Call() fails!")
	}

	Workingmap[nodeID].IntermediateFileList = append(Workingmap[nodeID].IntermediateFileList, intermediateFile...)

	client.Close()
}

func findFileWithPrefix(prefix string, Filemap map[string]*FileMetadata) []string {
	var fileList []string

	for filename, _ := range Filemap {
		if strings.Contains(Config.DecodeFileName(filename), prefix) {
			fileList = append(fileList, Config.DecodeFileName(filename))
		}
	}

	return fileList
}

///////////////////////////////////Helper functions////////////////////////////
func getFileListFromCacheMap(cacheMap map[string][]string) []string {
	res := []string{}
	for cache, _ := range cacheMap {
		res = append(res, cache)
	}

	return res
}

func getCacheMapFromWorkingmap(Workingmap map[string]*WorkerInfo) map[string][]string {
	var res map[string][]string
	res = make(map[string][]string)

	for nodeID, wi := range Workingmap {
		for _, cache := range wi.IntermediateFileList {
			if _, ok := res[cache]; ok {
				res[cache] = append(res[cache], nodeID)
			} else {
				res[cache] = []string{nodeID}
			}
		}
	}

	return res
}

func getSubCacheMap(fileListPerTask []string, cacheMap map[string][]string) map[string][]string {
	var res map[string][]string
	res = make(map[string][]string)

	for _, filename := range fileListPerTask {
		for key, value := range cacheMap {
			if key == filename {
				res[filename] = value
				break
			}
		}
	}

	return res
}

func rangePartition(fileList []string, totalTask int, taskType string, exe_name string, output string, cacheMap map[string][]string) []*Task {
	var taskList []*Task

	fileListLen := len(fileList)

	num_files := fileListLen / totalTask
	extra := fileListLen % totalTask

	remain := fileListLen

	for i := 0; i < totalTask; i++ {
		var fileListPerTask []string

		fileListPerTask = append(fileListPerTask, fileList[fileListLen-remain:fileListLen-remain+num_files]...)

		remain -= num_files
		if extra != 0 {
			fileListPerTask = append(fileListPerTask, fileList[fileListLen-remain])
			remain--
			extra--
		}

		var task Task

		if cacheMap == nil {
			task = Task{i, taskType, exe_name, time.Now(), fileListPerTask, nil ,output}
		} else {
			subCacheMap := getSubCacheMap(fileListPerTask, cacheMap)
			task = Task{i, taskType, exe_name, time.Now(), fileListPerTask, subCacheMap, output}
		}

		taskList = append(taskList, &task)
	}

	return taskList
}

func hashPartition(fileList []string, totalTask int, taskType string, exe_name string, output string, cacheMap map[string][]string) []*Task {
	var taskList []*Task = make([]*Task, totalTask)

	var fileListPerTask [][]string = make([][]string, totalTask)

	// fileListLen := len(fileList)
	// num_files := fileListLen / totalTask

	for _, fileName := range fileList {
		parseName := strings.Split(fileName, "_")
		key := parseName[1]

		hashVal := int(Config.Hash(key)) % totalTask
		//fmt.Printf("Hash value for key %s is %d", key, hashVal)

		fileListPerTask[hashVal] = append(fileListPerTask[hashVal], fileName)
	}

	for i := 0; i < totalTask; i++ {

		var task Task

		if cacheMap == nil {
			task = Task{i, taskType, exe_name, time.Now(), fileListPerTask[i], nil, output}
		} else {
			subCacheMap := getSubCacheMap(fileListPerTask[i], cacheMap)
			task = Task{i, taskType, exe_name, time.Now(), fileListPerTask[i], subCacheMap, output}	
		}


		taskList = append(taskList, &task)
	}

	return taskList
}



///////////////////////////////////////MP3/////////////////////////////////////////
//////////////////////////////////////////Functions////////////////////////////////////////////

func RunNamenodeServer() {

	<-OpenNamenodeServer

	var namenode = new(Namenode)

	//Initialize all maps
	namenode.Filemap = make(map[string]*FileMetadata)
	namenode.Nodemap = make(map[string][]string)
	namenode.Workingmap = make(map[string]*WorkerInfo)

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

	getCurrentMaps(namenode.Filemap, namenode.Nodemap, namenode.Workingmap)

	go ListenOnFailedNode(namenode.Filemap, namenode.Nodemap, namenode.Workingmap)
	go ListenOnNewNode(namenode.Workingmap)

	fmt.Printf("===RunNamenodeServer: Listen on port %s\n", Config.NamenodePort)
	err = http.Serve(listener, mux)
	if err != nil {
		log.Fatal("Serve(listener, nil) error: ", err)
	}
}

func ListenOnFailedNode(Filemap map[string]*FileMetadata, Nodemap map[string][]string, Workingmap map[string]*WorkerInfo) {
	for {
		failedNodeID := <-UpdateFilemapChan

		//If failed nodeID can be found in Nodemap
		if reReplicaFileList, ok := Nodemap[failedNodeID]; ok {
			//delete from Nodemap
			delete(Nodemap, failedNodeID)

			//Also update Filemap and re-replicate files
			for _, filename := range reReplicaFileList {
				for idx, nodeID := range Filemap[filename].DatanodeList {
					if nodeID == failedNodeID {
						Filemap[filename].DatanodeList = append(Filemap[filename].DatanodeList[:idx], Filemap[filename].DatanodeList[idx+1:]...)
						break
					}
				}
				checkReplica(filename, Filemap[filename], Nodemap)
			}
		}

		//If failed nodeID was also working at a task
		if workerInfo, ok := Workingmap[failedNodeID]; ok {
			//delete from Workingmap
			delete(Workingmap, failedNodeID)

			if len(workerInfo.TaskList) != 0 {
				for _, unfinishedTask := range workerInfo.TaskList {
					TaskKeeperChan <- unfinishedTask
				}
			}
		}
	}
}

func ListenOnNewNode(Workingmap map[string]*WorkerInfo) {
	for true {
		NewNodeID := <-Mem.NewNodeChan

		privateChan := make(chan string)

		wi := WorkerInfo{[]*Task{}, []string{}, privateChan}

		Workingmap[NewNodeID] = &wi

		//TODO: can be better
		go waitForTaskChan(NewNodeID, Workingmap)
	}
}


/////////////////////////////////////////////RPC//////////////////////////////////
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

	datanodeList := getIdleDatanodeList(len(n.Filemap))

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
		for _, datanodeID := range filemetadata.DatanodeList {
			for idx, storedfilename := range n.Nodemap[datanodeID] {
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

func insert(filemap map[string]*FileMetadata, sdfsfilename string, datanodeID string) {
	if filemetadata, ok := filemap[sdfsfilename]; ok {
		//filemap[sdfsfilename] exist
		filemetadata.DatanodeList = append(filemetadata.DatanodeList, datanodeID)
		sort.Strings(filemetadata.DatanodeList)
	} else {
		//filemap[sdfsfilename] not exist
		newfilemetadata := FileMetadata{[]string{datanodeID}, time.Now()} //TODO Set LastWrtTime = time.Now() may cause some strange performance.
		filemap[sdfsfilename] = &newfilemetadata
	}
}

func checkReplica(sdfsfilename string, meta *FileMetadata, nodemap map[string][]string) {
	n := len(meta.DatanodeList)

	if n > Config.ReplicaNum-1 {
		//At least n = "ReplicaNum" datanodes store the sdfsfile
		return
	} else if n < 1 {
		//Debug use. Normally, this line will never be printed.
		fmt.Println("Wrong! File isn't stored in any datanodes.")
	} else {
		//Not enough replicas
		fmt.Println("Start re-replicating...")
		//defer Config.TimeCount()()

		neededReReplicaNum := Config.ReplicaNum - n

		sort.Strings(meta.DatanodeList)

		reReplicaNodeList, len := findDifferenceOfTwoLists(Mem.MembershipList, meta.DatanodeList, neededReReplicaNum)

		fmt.Println("The list of nodes to do reReplicate is: ", reReplicaNodeList)
		if len == 0 {
			//MembershipList == meta.DatanodeList, e.g. only 1 node in group
			return
		}

		//RPC meta.DatanodeList[0] to "PutSdfsfileToList"
		informDatanodeToPutSdfsfile(meta.DatanodeList[0], sdfsfilename, reReplicaNodeList) //Helper function at client.go

		//Update filemap
		meta.DatanodeList = append(meta.DatanodeList, reReplicaNodeList...)

		//Update nodemap
		for _, nodeID := range reReplicaNodeList {
			if _, ok := nodemap[nodeID]; ok {
				nodemap[nodeID] = append(nodemap[nodeID], sdfsfilename)
			} else {
				nodemap[nodeID] = []string{sdfsfilename}
			}
		}

		fmt.Println("Re-replication complete!")
	}
}

//This function first find the first same element in both sorted lists,
//and then returns N different numbers in bigList starting from that element.
//Note: smallList is a subset of bigList
func findDifferenceOfTwoLists(bigList []string, smallList []string, N int) ([]string, int) {
	var startIdx int
	var res []string
	bigListLen := len(bigList)

	for ; startIdx < bigListLen; startIdx++ {
		if bigList[startIdx] == smallList[0] {
			break
		}
	}

	smallListIdx := 1

	for i := (startIdx + 1) % bigListLen; i != startIdx; i = (i + 1) % bigListLen {
		if N > 0 {
			if smallListIdx < len(smallList) && bigList[i] == smallList[smallListIdx] {
				smallListIdx++
				continue
			}
			res = append(res, bigList[i])
			N--
		} else {
			break
		}
	}

	return res, len(res)
}

func getCurrentMaps(filemap map[string]*FileMetadata, nodemap map[string][]string, Workingmap map[string]*WorkerInfo) {
	//RPC datenodes to get nodemap
	for _, nodeID := range Mem.MembershipList {
		nodeAddr := Config.GetIPAddressFromID(nodeID)

		client := NewClient(nodeAddr + ":" + Config.DatanodePort)
		client.Dial()

		var filelist []string
		client.rpcClient.Call("Datanode.GetFileList", Mem.LocalID, &filelist)

		nodemap[nodeID] = filelist

		privateChan := make(chan string)

		wi := WorkerInfo{[]*Task{}, []string{}, privateChan}

		Workingmap[nodeID] = &wi //TODO: when Master fail, get current task from other nodes

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
		checkReplica(sdfsfilename, filemetadata, nodemap)
	}
}

//Uniformly distribute file
func getIdleDatanodeList(fileIdx int) []string {
	var datanodeList []string

	nodeNum := len(Mem.MembershipList)

	for i := 0; i < Config.Min(Config.ReplicaNum, nodeNum); i++ {
		datanodeList = append(datanodeList, Mem.MembershipList[(Config.ReplicaNum*fileIdx+i)%nodeNum])
	}

	return datanodeList
}
