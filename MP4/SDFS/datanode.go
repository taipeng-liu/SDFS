package sdfs

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/exec"
	"strings"

	Config "../Config"
	Mem "../Membership"
)

type Datanode struct {
	NamenodeID string   //NodeID, not Address
	FileList   []string //list of sdfsfile
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

	go WaitingForFailedNodeID() //helper function at client.go

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
			EvokeNamenode(d.NamenodeID) //helper function at client.go
		}
	}

	*resp = Config.GetIPAddressFromID(d.NamenodeID)
	return nil
}

//This RPC method will be called from client.go when a node fail/leave
func (d *Datanode) UpdateNamenodeID(failedNodeID string, resp *bool) error {
	if d.NamenodeID != "" && failedNodeID != d.NamenodeID {
		//Namenode is still alive, don't update namenodeID
		*resp = false

		//If this datanode is namenode, update Filemap
		if d.NamenodeID == Mem.LocalID {
			UpdateFilemapChan <- failedNodeID
		}
	} else {
		//Namenode fails or no namenode, update namenodeID locally
		*resp = true
		d.NamenodeID = Mem.MembershipList[0]

		if d.NamenodeID == Mem.LocalID {
			OpenNamenodeServer <- ""
		}
	}

	return nil
}

func (d *Datanode) GetFileList(namenodeID string, res *[]string) error {
	d.NamenodeID = namenodeID
	*res = d.FileList
	return nil
}

//Save contents of "sdfsfile" from client
func (d *Datanode) Put(req PutRequest, resp *PutResponse) error {
	Config.CreateDirIfNotExist(Config.TempfileDir)

	encodedFileName := Config.EncodeFileName(req.Filename)

	var tempfilePath string

	tempfilePath := Config.TempfileDir + "/" + encodedFileName + "." + req.Hostname

	//Open and write
	var tempfile *File
	
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
		sdfsfilePath := Config.SdfsfileDir + "/" + encodedFileName

		if !req.AppendMode {
			os.Rename(tempfilePath, sdfsfilePath)
		} else {
			Config.AppendFileToFile(tempfilePath, sdfsfilePath)
		}

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
				if i == len(d.FileList)-1 {
					d.FileList = append(d.FileList, req.Filename)
				}
			}
		}

		fmt.Printf("Write sdfsfile %s succeed: size = %d, source = %s!!\n", req.Filename, filesize, req.Hostname)
		log.Printf("====Store sdfsfile: filename = %s, size = %d, source = %s\n", req.Filename, filesize, req.Hostname)
	}

	resp.Response = "ok"

	return nil
}


//Send contents of "sdfsfile" to client
func (d *Datanode) Get(req GetRequest, resp *GetResponse) error {

	encodedFileName := Config.EncodeFileName(req.Filename)

	sdfsfilepath := Config.SdfsfileDir + "/" + encodedFileName

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
			fmt.Printf("Read sdfsfile %s succeed!!\n", req.Filename)
			resp.Eof = true
		}
	}

	resp.Content = buf[:n]

	return nil
}

//Delete "sdfsfile"
func (d *Datanode) Delete(req DeleteRequest, resp *DeleteResponse) error {

	encodedFileName := Config.EncodeFileName(req.Filename)

	sdfsfilepath := Config.SdfsfileDir + "/" + encodedFileName

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

	fmt.Printf("Delete sdfsfile %s succeed!!\n", req.Filename)
	log.Printf("Datanode: Delete sdfsfile %s!!\n", req.Filename)

	return nil
}

func (d *Datanode) PutSdfsfileToList(req ReReplicaRequest, res *bool) error {
	var resp int

	for _, nodeID := range req.DatanodeList {
		nodeAddr := Config.GetIPAddressFromID(nodeID)

		go RpcOperationAt("put", req.Filename, req.Filename, nodeAddr, Config.DatanodePort, false, &resp, len(req.DatanodeList))
	}

	<-PutFinishChan

	return nil
}

func (d *Datanode) RunMapReduce(req Task, res *int) error {
	fmt.Printf("Datanode: Receive TaskID %d, TaskType %s, TaskExe %s\n", req.TaskID, req.TaskType, req.TaskExe)

	if req.TaskType == "map" {
		log.Printf("DataNode: Task %d Started!!\n", req.TaskID)

		// temp, err := os.Create(Config.LocalfileDir + "/" + Config.TempFile)
		// if err != nil {
		// 	fmt.Println("os.Create() error")
		// 	return err
		// }
		// defer temp.Close()

		fileNum := len(req.FileList)

		//Call MapFunc for each file
		for idx, fileName := range req.FileList {
			fmt.Printf("Start Process File %s\n", fileName)

			//Fetch SDFSfile to local file system
			GetFile(fileName, fileName)

			//Create temp.txt in LocalfileDir
			temp, err := os.Create(Config.LocalfileDir + "/" + Config.TempFile)
			if err != nil {
				fmt.Println("os.Create() error")
				return err
			}

			//Scan file
			data, err := os.Open(Config.LocalfileDir + "/" + fileName)
			if err != nil {
				log.Println("os.Open() error")
				return err
			}
			var scanner = bufio.NewScanner(data)

			var lineCnt = 0

			var buf = ""

			//TODO if EOF but lineCnt < 10 .....(taipeng2)
			for scanner.Scan() {
				// fmt.Println(scanner.Text())
				//Deal with EOF
				if lineCnt < 10 && scanner.hasNext() {
					strings.Join(buf, scanner.Text())
				} else {
					// MapFunc(req.TaskExe)

					_, err := temp.WriteString(buf)
					if err != nil {
						panic(err)
					}

					cmd := exec.Command("./"+req.TaskExe, temp)
					res, _ := cmd.Output()

					parseMapRes(res, Task.Output)

					defer temp.Close()
					lineCnt = 0
					buf = ""
				}
			}

		}

	}
}

/*
func (d *Datanode) MapFunc(mapEXE string) {

	encodedFileName := Config.EncodeFileName(req.Filename)

	sdfsfilepath := Config.SdfsfileDir + "/" + encodedFileName

	//Open file

	if err != nil {
		log.Printf("os.Open() can't open file %s\n", sdfsfilepath)
		return err
	}
	defer sdfsfile.Close()
}
*/

//xiangl14 TODO: How to parse Mapper output with absolutely different valaue types e.g. {"1":["5"],"2":["1","3"],"3":["4"],"4":["2"],"5":["6"],"6":["1"]}
func parseMapRes(res []byte, prefix string) error {
	s := string(res)

	isKey := true

	var key, val []byte

	for i := 0; i < len(s); i++ {
		if isKey {
			if s[i] == ':' {
				isKey = false
			} else {
				key = append(key, s[i])
			}
		} else {
			if s[i] == '\n' {
				MapperOutput(key, val, prefix)
			} else {
				val = append(val, s[i])
			}

		}

	}
	// var reader = strings.NewReader(s)

	// for reader.Read() {
	// 	if
	// }

	return nil
}

//xiangl14 TODO: GetFile then manually append it, then Putfile
func MapperOutput(key []byte, val []byte, prefix string) {

	PutFile()

}
