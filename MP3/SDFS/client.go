package sdfs

import (
	"log"
	"net/rpc"
	"fmt"
	"io/ioutil"

	Config "../Config"
)

const (
	R = 1
	W = 4
)

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

func GetDatanodeList(filename string) ([]string, int){
	var datanodeList []string
	var response Response

	//TODO: RPC Namenode and get response
	//TODO: Check response to get datanodeList

	return datanodeList, len(datanodeList)
}


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


////////////////Functions Called from main.go/////////////////////
func PutFile(filenames []string){

	localfilename := filenames[0]
	sdfsfilename  := filenames[1]

	//RPC Namenode
	//Check if the file already exist
	datanodeList, n := GetDatanodeList(sdfsfilename)

	if n > 0 {
		//Exist
		//TODO: Update sdfsfile by localfile
		//RPC each Datanode from the list
		//When W datanode(s) send finished, return
	} else {
		//Not exist
		//TODO: Insert localfile into sdfsfile
		//RPC each Datanode from the list
		//When W datanode(s) send finished, return
	}	
}

func GetFile(filenames []string){

	localfilename := filename[1]
	sdfsfilename  := filename[0]

	//RPC Namenode
	//Namenode send back datanodes who is currently storing the file
	datanodeList, n := GetDatanodeList(sdfsfilename)

	if n > 0 {
		//Exist
		//TODO Download sdfsfile from each datanod and name it localfilename
		//When R datanode(s) send back the file, return
	} else {
		//Not exist
		fmt.Printf("Get error: no such sdfsfile %s\n", sdfsfilename)
	}
}

func DeleteFile(filenames []string){
	//RPC Namenode
	//Namenode send back datanodes who save the file
	toDelete := filename[0]
	datanodeList, n := GetDatanodeList(toDelete)

	if n == 0 {
		fmt.Printf("Delete error: no such sdfsfile %s\n", toDelete)
	}


	//RPC each datanode from the list
	for _, datanodeID := range datanodeList{
		datanodeAddr := Config.GetIPAddrFromID(datanodeID)
		//TODO RPC datanode
	}
	//TODO: When ALL datanodes send finished, return
	//Question: Is "ALL" neccessary?
}

func ShowDatanode(filenames []string){
	//RPC Namenode
	//Namenode send back datanodes who save the file
	toFind := filenames[0]
	datanodeList, n := GetDatanodeList(toFind)

	if n == 0 {
		fmt.Printf("Find error: no sdfsfile %s\n", toFind)
		return
	}

	//Print the list
	fmt.Printf("Servers who save the file %s:\n", toFind)
	for _, datanodeID := range datanodeList {
		fmt.Println(datanodeID)
	}
}

func ShowFile() {
	//List files in "MP3/LocalFile"
	listFile(Config.LocalfilePath)
	//List files in "MP3/SDFS/SDFSFile"
	listFile(Config.SdfsfilePath)
}
