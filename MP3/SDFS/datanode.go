package sdfs

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"

	Config "../Config"
	Mem "../Membership"
)

type Datanode struct {
	NamenodeID   string   //NodeID, not Address
	FileList     []string //list of sdfsfile
}

/////////////////////////////////////////Functions////////////////////////////////

func RunDatanodeServer() {
	var datanode = new(Datanode)
	datanode.FileList = []string{}

	datanodeServer := rpc.NewServer()

	err := datanodeServer.Register(datanode)
	if err != nil {
		log.Fatal("Register(datanode) error:", err)
	}

	//======For multiple servers=====
	oldMux := http.DefaultServeMux
	mux := http.NewServeMux()
	http.DefaultServeMux = mux
	//===============================

	datanodeServer.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)

	//=======For multiple servers=====
	http.DefaultServeMux = oldMux
	//================================

	listener, err := net.Listen("tcp", ":"+Config.DatanodePort)
	if err != nil {
		log.Fatal("Listen error", err)
	}

	go WaitingForFailedNodeID()    //helper function at client.go

	fmt.Printf("===RunDatanodeServer: Listen on port %s\n", Config.DatanodePort)
	err = http.Serve(listener, mux)
	if err != nil {
		log.Fatal("Serve(listener, nil) error: ", err)
	}

}

//////////////////////////////////////Methods///////////////////////////////////

func (d *Datanode) GetNamenodeAddr(req string, resp *string) error {
	//No namenode right now, start a selection process
	if d.NamenodeID == "" {
		//TODO New namenode election strategy
		d.NamenodeID = Mem.MembershipList[0]

		if d.NamenodeID == Mem.LocalID {
			//This datanode is namenode
			OpenNamenodeServer <- ""
		} else {
			//This datanode is not namenode, evoke namenode!
			EvokeNamenode(d.NamenodeID)  //helper function at client.go
		}
	}

	fmt.Printf("NamenodeID is: %s\n", d.NamenodeID)
	*resp = Config.GetIPAddressFromID(d.NamenodeID)
	return nil
}


//This RPC method will be called from client.go when a node fail/leave
func (d *Datanode) UpdateNamenodeID(failedNodeID string, resp *bool) error{
	if d.NamenodeID != "" && failedNodeID != d.NamenodeID {
		//Namenode is still alive, don't update namenodeID
		*resp = false

		//If this datanode is namenode, update Filemap
		if d.NamenodeID == Mem.LocalID {
			UpdateFilemapChan <- failedNodeID
		}
	}else {
		//Namenode fails or no namenode, update namenodeID locally
		*resp = true
		d.NamenodeID = Mem.MembershipList[0]

		if d.NamenodeID == Mem.LocalID {
			OpenNamenodeServer <- ""
		}
	}

	return nil
}

func (d *Datanode) GetFileList(namenodeID string, res *[]string) error{
	d.NamenodeID = namenodeID
	*res = d.FileList
	return nil
}

//Save contents of "sdfsfile" from client
func (d *Datanode) Put(req PutRequest, resp *PutResponse) error {
	Config.CreateDirIfNotExist(Config.TempfileDir)
	tempfilePath := Config.TempfileDir + "/" + req.Filename + "." + req.Hostname

	//Open and write
	tempfile, err := os.OpenFile(tempfilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Println("os.OpenFile() error")
		return err
	}

	if _, err = tempfile.WriteAt(req.Content, req.Offset); err != nil {
		log.Println("sdfsfile.WriteAt() error", err)
		return err
	}

	//Write EOF, save file
	if req.Eof {
		fi, _ := tempfile.Stat()
		filesize := int(fi.Size())
		Config.CreateDirIfNotExist(Config.SdfsfileDir)
		sdfsfilePath := Config.SdfsfileDir + "/" + req.Filename

		os.Rename(tempfilePath, sdfsfilePath)
		os.RemoveAll(Config.TempfileDir)

		//Append if not exist
		if len(d.FileList) == 0 {
			//Empty list
			d.FileList = append(d.FileList, req.Filename)
		} else {
			for i, storedFilename := range d.FileList {
				if storedFilename == req.Filename {
					break
				}
				if i == len(d.FileList) - 1 {
					d.FileList = append(d.FileList, req.Filename)
				}
			}
		}


		fmt.Printf("Store sdfsfile: filename = %s, size = %d, source = %s\n", sdfsfilePath, filesize, req.Hostname)
		log.Printf("====Store sdfsfile: filename = %s, size = %d, source = %s\n", sdfsfilePath, filesize, req.Hostname)
	}

	resp.Response = "ok"

	return nil
}

//Send contents of "sdfsfile" to client
func (d *Datanode) Get(req GetRequest, resp *GetResponse) error {
	sdfsfilepath := Config.SdfsfileDir + "/" + req.Filename

	//Open file
	sdfsfile, err := os.Open(sdfsfilepath)
	if err != nil {
		log.Printf("os.Open() can't open file %s\n", sdfsfilepath)
		return err
	}
	defer sdfsfile.Close()

	//Read file into resp
	buf := make([]byte, req.Size)

	n, err := sdfsfile.ReadAt(buf, req.Offset)
	if err != nil {
		if err != io.EOF {
			return err
		} else {
			resp.Eof = true
		}
	}

	resp.Content = buf[:n]

	return nil
}

//Delete "sdfsfile"
func (d *Datanode) Delete(req DeleteRequest, resp *DeleteResponse) error {

	sdfsfilepath := Config.SdfsfileDir + "/" + req.Filename

	if err := os.Remove(sdfsfilepath); err != nil {
		return err
	}

	//Assume deleted file can be found in FileList
	for idx, filename := range d.FileList {
		if filename == req.Filename {
			d.FileList = append(d.FileList[:idx], d.FileList[idx+1:]...)
			break
		}
	}

	return nil
}

func (d *Datanode) PutSdfsfileToList(req ReReplicaRequest, res *bool) error {
	var resp int

	for _, nodeID := range req.DatanodeList {
		nodeAddr := Config.GetIPAddressFromID(nodeID)

		go RpcOperationAt("put", req.Filename, req.Filename, nodeAddr, Config.DatanodePort, false, &resp)
	}

	for resp < len(req.DatanodeList) {
		//TODO timeout
		time.Sleep(time.Second)
	}
	
	return nil
}
