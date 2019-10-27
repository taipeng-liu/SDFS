package sdfs

import (
	"log"
	"net/rpc"
	"fmt"
	"io"
	"io/ioutil"
	"time"
	"os"

	Config "../Config"
)

const (
	R = 1
	W = 4
)

type Client struct {
	Addr       string
	rpcClient  *rpc.Client
}

///////////////////////////////////RPC Calls////////////////////////////


func NewClient(addr string) *Client {
	return &Client{Addr:addr}
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
	if err := c.rpcClient.Call("Namenode.GetDatanodeList", FindRequest{Filename: filename}, &res); err != nil{
		return []string{}, 0
	}

	return res.DatanodeList, len(res.DatanodeList)
}

func (c *Client) InsertFile(filename string) ([]string, int) {
	var res InsertResponse
	if err := c.rpcClient.Call("Namenode.InsertFile", InsertRequest{Filename: filename}, &res); err != nil{
		return []string{}, 0
	}

	return res.DatanodeList, len(res.DatanodeList)
}

func GetNamenodeAddr() string{
	var resp string

	client := NewClient("localhost" + ":" + Config.DatanodePort)
	client.Dial()

	if err := client.rpcClient.Call("Datanode.GetNamenodeAddr", "", &resp); err != nil{
		return ""
	}

	client.Close()

	return resp
}


func (c *Client) Put(localfilename string, sdfsfilename string) error{

	localfilepath := Config.LocalfileDir + "/" + localfilename

	//Get filesize and total blocks	
	fileStat, err := os.Stat(localfilepath)
	if err != nil {
		return err
	}

	fileSize := fileStat.Size()

	totalblock := int(fileSize/Config.BLOCK_SIZE)
	if fileSize%Config.BLOCK_SIZE != 0 {
		totalblock += 1
	}

	fi := FileInfo{Filename   : sdfsfilename,
		       Filesize   : fileSize,
		       Totalblock : totalblock,}

	//Open the file
	localfile, err := os.Open(localfilepath)
	if err != nil {
		log.Printf("os.Open() can't open file %s\n", localfilepath)
		return err
	}
	defer localfile.Close()

	//Send file by blocks
	buf := make([]byte, Config.BLOCK_SIZE)
	for blockIdx := 0; blockIdx < totalblock; blockIdx++ {
		n, err := localfile.ReadAt(buf, int64(blockIdx)*Config.BLOCK_SIZE)
		if err != nil && err != io.EOF {
			return err
		}

		block := Block{Idx    : blockIdx,
			       Size   : int64(n),
			       Content: buf[:n],}//TODO: Test: n or n+1
		req := PutRequest{FileInfo : fi,
				  Block    : block,}

		var res PutResponse
		if err = c.rpcClient.Call("Datanode.Put", req, &res); err != nil{
			return err
		}

		if res.Err != nil {
			return res.Err
		}
	}

	return nil
}

func (c *Client) Get(sdfsfilename string, localfilename string, addr string) error{
	Config.CreateDirIfNotExist(Config.TempfileDir)
	tempfilePath := Config.TempfileDir + "/" + localfilename + "." + addr

	tempfile, err := os.OpenFile(tempfilePath, os.O_RDWR|os.O_CREATE|os.APPEND, 0755)
	if err != nil {
		log.Println("os.OpenFile() error")
		return err
	}

	for blockIdx := 0; !eof; blockIdx++ {
		req := GetRequest{sdfsfilename, int64(blockIdx) * Config.BLOCK_SIZE, BLOCK_SIZE}
		var res GetResponse
		if err := c.rpcClient.Call("Datanode.Get", req, &res); err != nil{
			return err
		}

		eof = res.Eof
		
		if _, err = tempfile.WriteAt(res.Content, int64(blockIdx) * Config.BLOCK_SIZE); err != nil {
			log.Prinln("tempfile.WriteAt() error")
			return err
		}
	}

	os.Rename(tempfilePath, Config.LocalfileDir + "/" + localfilename)
	return nil
}

func (c *Client) Delete(sdfsfilename string) error{
	req := DeleteRequest{sdfsfilename}
	var res DeleteReponse

	if err := c.rpcClient.Call("Datanode.Delete", req, &res); err != nil{
		return err
	}
	return nil
}




/////////////////////Functions Called from main.go////////////////////////

func PutFile(filenames []string){

	localfilename := filenames[0]
	sdfsfilename  := filenames[1]

	//Check if localfile exists
	localfilePath := Config.LocalfileDir + "/" + localfilename
	if _, err := os.Stat(localfilePath); os.IsNotExist(err) {
		fmt.Printf("%s dose not exsit!\n", localfilePath)
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
	}

	//Upload localfile to datanodes
	var respCount int 

	for _, datanodeID := range datanodeList {
		datanodeAddr := Config.GetIPAddressFromID(datanodeID)
		go RpcOperationAt("put", localfilename, sdfsfilename, datanodeAddr, Config.DatanodePort, &respCount)
	}

	for (respCount < W && respCount < n) {
		//TODO: Set up timeout in case of no response causing forever waiting
		time.Sleep(time.Second)
	}
	
	client.Close()

	log.Println("PutFile successfully return")
	return
}

func GetFile(filenames []string){

	localfilename := filenames[1]
	sdfsfilename  := filenames[0]

	//Check if sdfsfile exist
	namenodeAddr := GetNamenodeAddr()
	client := NewClient(namenodeAddr + ":" + Config.NamenodePort)
	client.Dial()

	datanodeList, n := client.GetDatanodeList(sdfsfilename)

	if n == 0{
		//No datanode store sdfsfile, return
		log.Printf("Get error: no such sdfsfile %s\n", sdfsfilename)
	}

	//Download sdfsfile from datanode
	var respCount int

	for _, datanodeID := range datanodeList {
		datanodeAddr := Config.GetIPAddressFromID(datanodeID)
		go RpcOperationAt("get", localfilename, sdfsfilename, datanodeAddr, Config.DatanodePort, &respCount)
	}

	for (resCount < R && resCount < n){
		//TODO timeout
		time.Sleep(time.Second)
	}

	client.Close()

	//Clear all .tmp file
	err := os.RemoveAll(Config.TempFileDir)
	if err != nil {
		log.Println("RemoveAll() error: can't remove tempFileDir")
	}

	log.Println("GetFile successfully return")
	return
}

func DeleteFile(filenames []string){
	
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
	var respCount int

	for _, datanodeID := range datanodeList{
		datanodeAddr := Config.GetIPAddressFromID(datanodeID)
		go RpcOperationAt("delete", "", sdfsfilename, datanodeAddr, Config.DatanodePort, &respCount)
	}
	
	for respCount < n {
		//TODO timeout
		time.Sleep(time.Second)
	}

	client.Close()
	log.Println("DeleteFile() successfully return")
	return
}

func ShowDatanode(filenames []string){
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
	fmt.Printf("Servers who save the file %s:\n", sdfsfilename)
	for _, datanodeID := range datanodeList {
		fmt.Println(datanodeID)
	}

	client.Close()
}

func ShowFile() {
	//listFile(Config.LocalfileDir)
	listFile(Config.SdfsfileDir)
}


///////////////////////////////////Helper functions/////////////////////////////////////////

func listFile(dirPath string) {
	fmt.Printf("%s contains following files:\n", dirPath)

	files, err := ioutil.ReadDir(dirPath)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		fmt.Println(file.Name())
	}
}

func RpcOperationAt(operation string, localfilename string, sdfsfilename string, addr string, port string, respCount *int){
	client := NewClient(addr + ":" + port)
	client.Dial()

	switch operation {
		case "put":
			client.Put(localfilename, sdfsfilename)
		case "get":
			client.Get(sdfsfilename, localfilename, addr)
		case "delete":
			client.Delete(sdfsfilename)
		default:
			log.Println("RpcOperationAt(): Don't support this operation")
	}

	(*respCountPt)++          //TODO: This line is a critical section, use mutex

	client.Close()
}



