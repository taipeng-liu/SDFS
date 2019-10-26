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


func (c *Client) Put(localfilename string) (error, string) (
	//TODO
	//Get filepath
	//Open file and read in buf[BLOCK_SIZE]
	//Create FileInfo and Block
	//Send putrequest
	//iterate the above precedures until read EOF
	localfilepath := Config.LocalfileDir + "/" + localfilename
	
	
}

func (c *Client) Get() () {
	//TODO
}

func (c *Client) Delete() () {
	//TODO
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

func PutFileAt(localfilename string, addr string, port string, respCountPt *int){
	client := NewClient(addr + ":" + port)
	client.Dial()

	client.Put(localfilename)
	(*respCountPt)++          //TODO: This line is a critical section, use mutex

	client.Close()
}

func GetFileAt() {

}


func DeleteFileAt() {

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

/////////////////////Functions Called from main.go////////////////////////

func PutFile(filenames []string){

	localfilename := filenames[0]
	sdfsfilename  := filenames[1]

	namenodeAddr := GetNamenodeAddr()
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
		//Question: Synchronizely uploading?
		go PutFileAt(localfilename, datanodeAddr, Config.DatanodePort, &respCount)
	}

	while respCount < W {
		//Waiting for W response(s)
		//Check the condition every second
		//TODO: Set up timeout in case of no response causing forever waiting
		time.Sleep(time.Second())
	}
	
	client.Close()

	log.Println("PutFile successfully return")
	return
}

func GetFile(filenames []string){

	localfilename := filenames[1]
	sdfsfilename  := filenames[0]

	namenodeAddr := GetNamenodeAddr()
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
	sdfsfilename := filenames[0]

	namenodeAddr := GetNamenodeAddr()
	client := NewClient(namenodeAddr + ":" + Config.NamenodePort)
	client.Dial()

	datanodeList, n := client.GetDatanodeList(sdfsfilename)

	if n == 0 {
		fmt.Printf("Delete error: no such sdfsfile %s\n", sdfsfilename)
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
	//List files in "MP3/SDFS/localFile"
	listFile(Config.LocalfileDir)

	//List files in "MP3/SDFS/sdfsFile"
	listFile(Config.SdfsfileDir)
}
