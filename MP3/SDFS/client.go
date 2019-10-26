package sdfs

import (
	"log"
	"net/rpc"
	"fmt"
	"io/ioutil"
	"time"

	Config "../Config"
)

const (
	R = 1
	W = 3
)

type Client struct {
	Addr       string
	rpcClient  *rpc.Client
}

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


func (c *Client) Put(filename string) (error, string) (
	//TODO
	//Open file and read in buf[BLOCK_SIZE]
	//Create FileInfo and Block
	//Send putrequest
	//iterate the above precedures until read EOF
}

func (c *Client) Get() () {
	//TODO
}

func (c *Client) Delete() () {
	//TODO
}

///////////////////////////////////Helper functions/////////////////////////////////////////

/*
func RequestDatanode(reqType string, request Request, datanodeAddr string, port string) Response{
	var response Response
	
	client, err := rpc.DailHTTP("tcp", datanodeAddr + ":" + port)
	if err != nil {
		log.Fatal("Connection error: ", err)
	}
	
	switch reqType {
		case "put":
			client.Call("Datanode.Put", request, &response)
		case "get":
			client.Call("Datanode.Get", request, &response)
		case "delete":
			client.Call("Datanode.Delete", request, &response)
		default:
			fmt.Println("Unsupported request type!")
	}

	return response
}
*/


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

func PutFileAt(filename string, addr string, port string, respCountPt *int){
	client := NewClient(addr + ":" + port)
	client.Dial()

	client.Put(filename)
	(*respCountPt)++          //TODO: This line is a critical section, use mutex

	client.Close()
}

/////////////////////Functions Called from main.go////////////////////////

func PutFile(filenames []string){

	localfilename := filenames[0]
	sdfsfilename  := filenames[1]
	namenodeAddr := GetNamenodeAddr() //TODO: implement GetNamenodeAddr somewhere
	client := NewClient(namenodeAddr + ":" + Config.NamenodePort)
	client.Dial()

	//RPC Namenode
	//Check if the file already exist
	datanodeList, n := client.GetDatanodeList(sdfsfilename)

	if n == 0 {
		//Not exist
		datanodeList, n = client.InsertFile(sdfsfilename)
		if n == 0 {
			log.Println("====Insert sdfsfile error")
			return
		}
	}

	//RPC each Datanode from the list
	//When W datanode(s) send finished, return

	var respCount int 

	for _, datanodeID := range datanodeList {
		datanodeAddr := Config.GetIPAddressFromID(datanodeID)
		//Question: Is goroutine applicable for synchronizely uploading?
		go PutFileAt(localfilename, datanodeAddr, Config.DatanodePort, &respCount)
	}

	while respCount < W {
		//Waiting for W response(s)
		//TODO
		//Set up time out in case of no response causing forever waiting
		time.Sleep(time.Second())
	}
	
	client.Close()

	log.Println("PutFile successfully return")
	return
}

func GetFile(filenames []string){

	localfilename := filenames[1]
	sdfsfilename  := filenames[0]
	namenodeAddr := GetNamenodeAddr() //TODO: implement GetNamenodeAddr somewhere
	client := NewClient(namenodeAddr + ":" + Config.NamenodePort)
	client.Dial()

	//RPC Namenode
	//Namenode send back datanodes who is currently storing the file
	datanodeList, n := client.GetDatanodeList(sdfsfilename)

	if n > 0 {
		//Exist
		//TODO Download sdfsfile from each datanod and name it localfilename
		//When R datanode(s) send back the file, return
	} else {
		//Not exist
		fmt.Printf("Get error: no such sdfsfile %s\n", sdfsfilename)
	}

	client.Close()
}

func DeleteFile(filenames []string){
	//RPC Namenode
	//Namenode send back datanodes who save the file
	toDelete := filenames[0]

	namenodeAddr := GetNamenodeAddr() //TODO: implement GetNamenodeAddr somewhere
	client := NewClient(namenodeAddr + ":" + Config.NamenodePort)
	client.Dial()

	datanodeList, n := client.GetDatanodeList(toDelete)

	if n == 0 {
		fmt.Printf("Delete error: no such sdfsfile %s\n", toDelete)
	}


	//RPC each datanode from the list
	for _, datanodeID := range datanodeList{
		datanodeAddr := Config.GetIPAddressFromID(datanodeID)
		//TODO RPC datanode
	}
	//TODO: When ALL datanodes send finished, return
	//Question: Is "ALL" neccessary?

	client.Close()
}

func ShowDatanode(filenames []string){
	//RPC Namenode
	//Namenode send back datanodes who save the file
	toFind := filenames[0]

	namenodeAddr := GetNamenodeAddr() //TODO: implement GetNamenodeAddr somewhere
	client := NewClient(namenodeAddr + ":" + Config.NamenodePort)
	client.Dial()

	datanodeList, n := client.GetDatanodeList(toFind)
	if n == 0 {
		fmt.Printf("Find error: no sdfsfile %s\n", toFind)
		return
	}
	
	//Print the list
	fmt.Printf("Servers who save the file %s:\n", toFind)
	for _, datanodeID := range datanodeList {
		fmt.Println(datanodeID)
	}

	client.Close()
}

func ShowFile() {
	//List files in "MP3/SDFS/localFile"
	listFile(Config.LocalfilePath)

	//List files in "MP3/SDFS/sdfsFile"
	listFile(Config.SdfsfilePath)
}
