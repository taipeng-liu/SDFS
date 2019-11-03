package sdfs

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sync"
	"time"

	Config "../Config"
	Mem "../Membership"
)

const (
	R = 1
	W = Config.ReplicaNum
)

var KillTimeOut30s chan string = make(chan string)
var YESorNO chan bool = make(chan bool)
var PutFinishChan chan string = make(chan string)
var GetFinishChan chan string = make(chan string)
var DeleteFinishChan chan string = make(chan string)
var AllFilePutFinishChan chan string = make(chan string)
var mutex sync.Mutex
var fileCountMutex sync.Mutex

type Client struct {
	Addr      string
	rpcClient *rpc.Client
}

///////////////////////////////////RPC Calls////////////////////////////

func NewClient(addr string) *Client {
	return &Client{Addr: addr}
}

func (c *Client) Dial() error {
	client, err := rpc.DialHTTP("tcp", c.Addr)
	if err != nil {
		return err
	}
	c.rpcClient = client

	return nil
}

func (c *Client) Close() error {
	return c.rpcClient.Close()
}

func (c *Client) GetDatanodeList(filename string) ([]string, int) {
	var res FindResponse
	if err := c.rpcClient.Call("Namenode.GetDatanodeList", FindRequest{Filename: filename}, &res); err != nil {
		return []string{}, 0
	}

	return res.DatanodeList, len(res.DatanodeList)
}

func (c *Client) InsertFile(filename string) ([]string, int) {
	var res InsertResponse
	if err := c.rpcClient.Call("Namenode.InsertFile", InsertRequest{filename, Mem.LocalID}, &res); err != nil {
		return []string{}, 0
	}

	return res.DatanodeList, len(res.DatanodeList)
}

func (c *Client) GetWritePermission(sdfsfilename string) bool {
	var permitted bool
	if err := c.rpcClient.Call("Namenode.GetWritePermission", PermissionRequest{sdfsfilename, false}, &permitted); err != nil {
		return false
	}

	if !permitted {
		fmt.Println("Last write is within 60s. Do you still want to write? Please response in 30s. (y/n)")
		go TimeOut30s()
		MustWrite := <-YESorNO
		if MustWrite {
			err := c.rpcClient.Call("Namenode.GetWritePermission", PermissionRequest{sdfsfilename, true}, &permitted)
			if err != nil {
				return false
			}
		}
	}

	return permitted
}

func (c *Client) Put(localfilename string, sdfsfilename string, isLocal bool) error {

	var localfilepath string

	if isLocal {
		localfilepath = Config.LocalfileDir + "/" + localfilename
	} else {
		localfilepath = Config.SdfsfileDir + "/" + localfilename
	}

	//Get fileInfo
	fileInfo, err := os.Stat(localfilepath)
	if err != nil {
		return err
	}

	fileSize := fileInfo.Size()
	fmt.Printf("Put: filename = %s, size = %d, destination = %s\n", localfilepath, int(fileSize), c.Addr)
	log.Printf("====Put: filename = %s, size = %d, destination = %s\n", localfilepath, int(fileSize), c.Addr)

	//Open the file
	localfile, err := os.Open(localfilepath)
	if err != nil {
		log.Printf("os.Open() can't open file %s\n", localfilepath)
		return err
	}
	defer localfile.Close()

	//Send file by blocks
	buf := make([]byte, Config.BLOCK_SIZE)
	eof := false
	hostname := Config.GetHostName()

	for blockIdx := 0; !eof; blockIdx++ {
		offset := int64(blockIdx) * Config.BLOCK_SIZE

		n, err := localfile.ReadAt(buf, offset)
		if err != nil {
			if err != io.EOF {
				return err
			} else {
				eof = true
			}
		}

		req := PutRequest{sdfsfilename, eof, offset, buf[:n], hostname}

		var res PutResponse
		if err = c.rpcClient.Call("Datanode.Put", req, &res); err != nil {
			return err
		}

		if res.Response != "ok" {
			log.Println(res.Response)
			break
		}
	}

	return nil
}

func (c *Client) Get(sdfsfilename string, localfilename string, addr string) error {
	Config.CreateDirIfNotExist(Config.TempfileDir)

	tempfilePath := Config.TempfileDir + "/" + localfilename + "." + addr

	tempfile, err := os.OpenFile(tempfilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Println("os.OpenFile() error")
		return err
	}

	eof := false

	for blockIdx := 0; !eof; blockIdx++ {
		req := GetRequest{sdfsfilename, int64(blockIdx) * Config.BLOCK_SIZE, Config.BLOCK_SIZE}
		var res GetResponse
		if err := c.rpcClient.Call("Datanode.Get", req, &res); err != nil {
			return err
		}

		eof = res.Eof

		if _, err = tempfile.WriteAt(res.Content, int64(blockIdx)*Config.BLOCK_SIZE); err != nil {
			log.Println("tempfile.WriteAt() error")
			return err
		}
	}

	filePath := Config.LocalfileDir + "/" + localfilename

	fi, _ := tempfile.Stat()
	filesize := int(fi.Size())

	Config.CreateDirIfNotExist(Config.LocalfileDir)
	os.Rename(tempfilePath, filePath)

	fmt.Printf("Get file: filename = %s, size = %d, source = %s\n", filePath, filesize, addr)
	log.Printf("Get file: filename = %s, size = %d, source = %s\n", filePath, filesize, addr)

	return nil
}

func (c *Client) Delete(sdfsfilename string) error {
	req := DeleteRequest{sdfsfilename}
	var res DeleteResponse

	if err := c.rpcClient.Call("Datanode.Delete", req, &res); err != nil {
		return err
	}
	return nil
}

func (c *Client) DeleteFileMetadata(sdfsfilename string) error {
	var res bool

	if err := c.rpcClient.Call("Namenode.DeleteFileMetadata", sdfsfilename, &res); err != nil {
		return err
	}
	if !res {
		fmt.Printf("Can't find filemetadata of %s in Filemap\n", sdfsfilename)
	}

	return nil
}

/////////////////////Functions Called from main.go////////////////////////
func PutFileOrPutDir(filenames []string) {

	if len(filenames) < 2 {
		fmt.Println("Usage: put [localfilename] [sdfsfilename]")
		return
	}

	localfilePath := Config.LocalfileDir + "/" + filenames[0]

	fi, err := os.Stat(localfilePath)

	//Check if localfile exists
	if os.IsNotExist(err) {
		fmt.Printf("===Error: %s does not exsit in local!\n", localfilePath)
		log.Printf("===Error: %s does not exsit in local!\n", localfilePath)
		return
	}

	//Check file mode
	switch mode := fi.Mode(); {
	case mode.IsDir():
		fmt.Println("Running PutDir...")
		PutDir(filenames)
	case mode.IsRegular():
		fmt.Println("Running PutFile...")
		var notUsed int
		PutFile(filenames, false, &notUsed, 1)
	}
}

func PutDir(filenames []string) {

	localdirname := filenames[0]
	sdfsdirname := filenames[1]

	localdirPath := Config.LocalfileDir + "/" + localdirname

	//Get the list of files in localdirPath
	files, err := ioutil.ReadDir(localdirPath)
	if err != nil {
		log.Fatal(err)
	}

	//For each files in localdirPath, run PutFile()
	defer Config.TimeCount()()
	totalFiles := len(files)
	var fileCount int

	for _, file := range files {
		subfilenames := []string{localdirname + "/" + file.Name(), sdfsdirname + "/" + file.Name()} 
		go PutFile(subfilenames, true, &fileCount, totalFiles)
	}

	<-AllFilePutFinishChan

	fmt.Println("PutDir successfully return")
	log.Println("====PutDir successfully return")

	return
}

//put command: put [localfilename] [sdfsfilename]
func PutFile(filenames []string, fromDir bool, fileCount *int, totalFiles int) {

	//localfilename or sdfsfilename
	localfilename := filenames[0]
	sdfsfilename := filenames[1]
	var localfilePath string

	localfilePath = Config.LocalfileDir + "/" + localfilename

	//Check if localfile exists
	if _, err := os.Stat(localfilePath); os.IsNotExist(err) {
		fmt.Printf("===Error: %s does not exsit in local!\n", localfilePath)
		log.Printf("===Error: %s does not exsit in local!\n", localfilePath)
		return
	}

	//Check if sdfsfile exist
	namenodeAddr := GetNamenodeAddr()

	client := NewClient(namenodeAddr + ":" + Config.NamenodePort)
	client.Dial()

	datanodeList, n := client.GetDatanodeList(sdfsfilename)

	if n == 0 {
		//No datanode store this sdfsfile, insert it
		datanodeList, n = client.InsertFile(sdfsfilename)
		if n == 0 {
			log.Println("====Insert sdfsfile error")
			return
		}
	} else {
		//Before writing, RPC namenode to get write permission
		if canWrite := client.GetWritePermission(sdfsfilename); !canWrite {
			return
		}
	}

	//Shared Variable: Write Quorum for uploading localfile to datanodes
	var respCount int = 0
	defer Config.TimeCount()()

	for _, datanodeID := range datanodeList {
		datanodeAddr := Config.GetIPAddressFromID(datanodeID)
		go RpcOperationAt("put", localfilename, sdfsfilename, datanodeAddr, Config.DatanodePort, true, &respCount, n)
	}

	<-PutFinishChan

	client.Close()

	if fromDir {
		//Add 1 to fileCount
		fileCountMutex.Lock()
		(*fileCount)++
		fileCountMutex.Unlock()

		if (*fileCount) == totalFiles {
			AllFilePutFinishChan <- ""
		}
	} else {
		fmt.Println("PutFile successfully return")
		log.Println("====PutFile successfully return")
	}

	return
}

//get command: get [sdfsfilename] [localfilename]
func GetFile(filenames []string) {
	if len(filenames) < 2 {
		fmt.Println("Usage: get [sdfsfilename] [localfilename]")
		return
	}

	localfilename := filenames[1]
	sdfsfilename := filenames[0]

	//Check if sdfsfile exist
	namenodeAddr := GetNamenodeAddr()
	client := NewClient(namenodeAddr + ":" + Config.NamenodePort)
	client.Dial()

	datanodeList, n := client.GetDatanodeList(sdfsfilename)

	if n == 0 {
		//No datanode store sdfsfile, return
		fmt.Printf("Get error: no sdfsfile %s\n", sdfsfilename)
		log.Printf("Get error: no sdfsfile %s\n", sdfsfilename)
		return
	}

	//Download sdfsfile from datanode
	var respCount int = 0
	defer Config.TimeCount()()

	for _, datanodeID := range datanodeList {
		datanodeAddr := Config.GetIPAddressFromID(datanodeID)
		//Todo:
		go RpcOperationAt("get", localfilename, sdfsfilename, datanodeAddr, Config.DatanodePort, true, &respCount, n)
	}

	<-GetFinishChan

	client.Close()

	//Clear all .tmp file
	err := os.RemoveAll(Config.TempfileDir)
	if err != nil {
		log.Println("RemoveAll() error: can't remove TempfileDir")
	}

	fmt.Println("GetFile successfully return")
	log.Println("====GetFile successfully return")

	return
}

// delete command: delete sdfsfilename
func DeleteFile(filenames []string) {
	if len(filenames) < 1 {
		fmt.Println("Usage: delete [sdfsfilename]")
	}

	sdfsfilename := filenames[0]

	//Check if sdfsfile exist
	namenodeAddr := GetNamenodeAddr()
	client := NewClient(namenodeAddr + ":" + Config.NamenodePort)
	client.Dial()

	datanodeList, n := client.GetDatanodeList(sdfsfilename)
	if n == 0 {
		log.Printf("Delete error: no such sdfsfile %s\n", sdfsfilename)
		return
	}

	//Delete sdfsfile in each datanode
	var respCount int = 0
	defer Config.TimeCount()()

	for _, datanodeID := range datanodeList {
		datanodeAddr := Config.GetIPAddressFromID(datanodeID)
		go RpcOperationAt("delete", "", sdfsfilename, datanodeAddr, Config.DatanodePort, true, &respCount, n)
	}

	<-DeleteFinishChan

	if err := client.DeleteFileMetadata(sdfsfilename); err != nil {
		log.Println("DeleteFileMetedata() error")
		return
	}

	client.Close()
	fmt.Println("DeleteFile successfully return")
	log.Println("DeleteFile successfully return")

	return
}

//ls sdfsfilename command: list all machine (VM) addresses where this file is currently being stored
func ShowDatanode(filenames []string) {
	if len(filenames) < 1 {
		fmt.Println("Usage: ls [sdfsfilename]")
		return
	}

	//Rpc Namenode which send back datanodeList
	sdfsfilename := filenames[0]

	namenodeAddr := GetNamenodeAddr()
	client := NewClient(namenodeAddr + ":" + Config.NamenodePort)
	client.Dial()

	datanodeList, n := client.GetDatanodeList(sdfsfilename)
	if n == 0 {
		fmt.Printf("Find error: no sdfsfile %s\n", sdfsfilename)
		return
	}

	//Print the list
	fmt.Printf("Datanodes who save the file %s:\n", sdfsfilename)
	log.Printf("Datanodes who save the file %s:\n", sdfsfilename)
	for _, datanodeID := range datanodeList {
		fmt.Println(datanodeID)
	}

	client.Close()
}

//store command: At any machine, list all files currently being stored at this machine
func ShowFile() {
	//listFile(Config.LocalfileDir) //Only for debugging, comment OUT in demo!
	listFile(Config.SdfsfileDir)
}

//clear command: Remove all sdfsfiles stored in "SDFS/sdfsFile"
func Clear() {
	err := os.RemoveAll(Config.SdfsfileDir)
	if err != nil {
		log.Println("Clear() error")
		return
	}
}

///////////////////////////////////Helper functions/////////////////////////////////////////
func TimeOut30s() {
	n := 0
	for ; n < 30; n++ {
		select {
		case <-KillTimeOut30s:
			return
		default:
			time.Sleep(time.Second)
		}
	}
	if n == 30 {
		fmt.Println("30s times out!")
		YESorNO <- false
	}
}

func GetNamenodeAddr() string {
	var resp string

	client := NewClient("localhost" + ":" + Config.DatanodePort)
	client.Dial()

	if err := client.rpcClient.Call("Datanode.GetNamenodeAddr", "", &resp); err != nil {
		return ""
	}

	client.Close()

	return resp
}

//Whenever client receive a filaedNodeID from updater, it calls datanode
func WaitingForFailedNodeID() {
	for true {
		failedNodeID := <-Mem.FailedNodeID

		var updateOK bool

		client := NewClient("localhost" + ":" + Config.DatanodePort)
		client.Dial()

		client.rpcClient.Call("Datanode.UpdateNamenodeID", failedNodeID, &updateOK)

		client.Close()
	}
}

func EvokeNamenode(namenodeID string) {
	namenodeAddr := Config.GetIPAddressFromID(namenodeID)

	client := NewClient(namenodeAddr + ":" + Config.DatanodePort)
	client.Dial()

	var updateOK bool
	client.rpcClient.Call("Datanode.UpdateNamenodeID", "", updateOK)
	if !updateOK {
		fmt.Println("UpdateNamenodeID error")
	}

	client.Close()
}

func listFile(dirPath string) {
	Config.CreateDirIfNotExist(dirPath)
	fmt.Printf("===%s contains following files:\n", dirPath)
	fmt.Println("===filename   size")

	files, err := ioutil.ReadDir(dirPath)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		decodedFileName := Config.DecodeFileName(file.Name())
		fmt.Printf("===%s    %d\n", decodedFileName, int(file.Size()))
	}
}

func informDatanodeToPutSdfsfile(datanodeID string, sdfsfilename string, otherNodeList []string) {
	datanodeAddr := Config.GetIPAddressFromID(datanodeID)

	client := NewClient(datanodeAddr + ":" + Config.DatanodePort)
	client.Dial()

	var res string
	client.rpcClient.Call("Datanode.PutSdfsfileToList", ReReplicaRequest{sdfsfilename, otherNodeList}, &res)

	client.Close()
}

func RpcOperationAt(operation string, localfilename string, sdfsfilename string, addr string, port string, isLocal bool, respCount *int, N int) {
	client := NewClient(addr + ":" + port)
	client.Dial()

	switch operation {
	case "put":
		client.Put(localfilename, sdfsfilename, isLocal)

		mutex.Lock()
		(*respCount)++
		mutex.Unlock()

		if *respCount == Config.Min(W, N) {
			PutFinishChan <- ""
		}
	case "get":
		client.Get(sdfsfilename, localfilename, addr)

		mutex.Lock()
		(*respCount)++
		mutex.Unlock()

		if *respCount == Config.Min(R, N) {
			GetFinishChan <- ""
		}
	case "delete":
		client.Delete(sdfsfilename)

		mutex.Lock()
		(*respCount)++
		mutex.Unlock()

		if *respCount == N {
			DeleteFinishChan <- ""
		}
	default:
		log.Println("RpcOperationAt(): Don't support this operation")
	}

	client.Close()
}
