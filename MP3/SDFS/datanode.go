package sdfs

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"

	Config "../Config"
)

var datanode = new(Datanode)

type Datanode struct {
	NamenodeAddr   string
	MembershipList []string
}

/////////////////////////////////////////Functions////////////////////////////////

func RunDatanodeServer() {
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

	fmt.Printf("===RunDatanodeServer: Listen on port %s\n", Config.DatanodePort)
	err = http.Serve(listener, mux)
	if err != nil {
		log.Fatal("Serve(listener, nil) error: ", err)
	}
}

func UpdataDatanode(newMemList []string) {
	datanode.MembershipList = newMemList
}

//***TODO: Start a new election and return new namnode's address
func UpdateMaster() {
	//Todo: Prune this algorithm?
	//For now, Always set the first in MembershipList as Master
	datanode.NamenodeAddr = Config.GetIPAddressFromID(datanode.MembershipList[0])

}

//Check if is this datanode is namenode
func IsMaster() bool {
	hostName := Config.GetHostName()
	return hostName == datanode.NamenodeAddr
}

//////////////////////////////////////Methods///////////////////////////////////

func (d *Datanode) GetNamenodeAddr(req string, resp *string) error {
	//No namenode right now, start a selection process
	if d.NamenodeAddr == "" {
		fmt.Println("Error!! no master!! Namenode Field is empty")
		// d.NamenodeAddr = NewElection()
	}
	fmt.Printf("Namenode Address is: %s!!\n", d.NamenodeAddr)
	*resp = d.NamenodeAddr
	return nil
}

func (d *Datanode) Put(req PutRequest, resp *PutResponse) error {
	Config.CreateDirIfNotExist(Config.TempfileDir)
	tempfilePath := Config.TempfileDir + "/" + req.Filename + "." + req.Hostname

	tempfile, err := os.OpenFile(tempfilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Println("os.OpenFile() error")
		return err
	}

	if _, err = tempfile.WriteAt(req.Content, req.Offset); err != nil {
		log.Println("sdfsfile.WriteAt() error", err)
		return err
	}

	if req.Eof {
		fi, _ := tempfile.Stat()
		filesize := int(fi.Size())
		Config.CreateDirIfNotExist(Config.SdfsfileDir)
		sdfsfilePath := Config.SdfsfileDir + "/" + req.Filename
		os.Rename(tempfilePath, sdfsfilePath)
		os.RemoveAll(Config.TempfileDir)

		fmt.Printf("Store sdfsfile: filename = %s, size = %d, source = %s\n", sdfsfilePath, filesize, req.Hostname)
		log.Printf("====Store sdfsfile: filename = %s, size = %d, source = %s\n", sdfsfilePath, filesize, req.Hostname)
	}

	resp.Response = "ok"

	return nil
}

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

func (d *Datanode) Delete(req DeleteRequest, resp *DeleteResponse) error {
	sdfsfilepath := Config.SdfsfileDir + "/" + req.Filename

	if err := os.Remove(sdfsfilepath); err != nil {
		return err
	}
	return nil
}
