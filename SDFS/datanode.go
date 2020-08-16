package sdfs

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
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

	tempfilePath = Config.TempfileDir + "/" + encodedFileName + "." + req.Hostname

	//Open and write
	tempfile, err := os.OpenFile(tempfilePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Println("Datanode.Put: os.OpenFile() error")
		return err
	}

	if _, err = tempfile.WriteAt(req.Content, req.Offset); err != nil {
		log.Println("Datanode.Out: sdfsfile.WriteAt() error", err)
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

		os.Remove(tempfilePath)

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

		// fmt.Printf("Write sdfsfile %s succeed: size = %d, source = %s!!\n", req.Filename, filesize, req.Hostname)
		log.Printf("====Store sdfsfile: filename = %s, size = %d, source = %s\n", req.Filename, filesize, req.Hostname)
	}

	resp.Response = "ok"

	return nil
}

//Send contents of "sdfsfile" to client
func (d *Datanode) Get(req GetRequest, resp *GetResponse) error {

	fileName := req.Filename

	if !strings.Contains(fileName, "cache") {
		fileName = Config.EncodeFileName(req.Filename)
	}

	sdfsfilepath := Config.SdfsfileDir + "/" + fileName

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
			// fmt.Printf("Read sdfsfile %s succeed!!\n", req.Filename)
			resp.Eof = true
		}
	}

	resp.Content = buf[:n]

	return nil
}

//Delete "sdfsfile"
func (d *Datanode) Delete(req DeleteRequest, resp *DeleteResponse) error {
	//fmt.Println("Enter Delete")
	fi, err := os.Stat(Config.SdfsfileDir + "/" + req.Filename)
	if os.IsNotExist(err) {
		fmt.Printf("===Datanode.Delete.Error: %s does not exsit in local!\n", req.Filename)
		log.Printf("===Delete Error: %s does not exsit in local!\n", req.Filename)
		return err
	}

	switch mode := fi.Mode(); {
	case mode.IsDir():
		err := os.RemoveAll(Config.SdfsfileDir + "/" + req.Filename)
		if err != nil {
			fmt.Println("Datanode.Delete: os.RemoveAll Error!")
			return err
		}

	case mode.IsRegular():
		encodedFileName := Config.EncodeFileName(req.Filename)

		sdfsfilepath := Config.SdfsfileDir + "/" + encodedFileName

		if err := os.Remove(sdfsfilepath); err != nil {
			return err
		}

		//Assume deleted file can be found in FileList
		//TODO: cache file exist in File:ist?
		for idx, filename := range d.FileList {
			if filename == req.Filename {
				d.FileList = append(d.FileList[:idx], d.FileList[idx+1:]...)
				break
			}
		}
	}

	//fmt.Printf("Delete sdfsfile %s succeed!!\n", req.Filename)
	log.Printf("Datanode: Delete sdfsfile %s!!\n", req.Filename)

	return nil
}

func (d *Datanode) PutSdfsfileToList(req ReReplicaRequest, res *bool) error {
	var resp int

	for _, nodeID := range req.DatanodeList {
		nodeAddr := Config.GetIPAddressFromID(nodeID)

		go RpcOperationAt("put", req.Filename, req.Filename, nodeAddr, Config.DatanodePort, false, &resp, len(req.DatanodeList), false)
	}

	<-PutFinishChan

	return nil
}

func (d *Datanode) RunMapReduce(req Task, res *int) error {
	fmt.Printf("Datanode: Receive TaskID %d, TaskType %s, TaskExe %s\n", req.TaskID, req.TaskType, req.TaskExe)

	if req.TaskType == Config.Map {
		log.Printf("DataNode: Map Task %d Started!!\n", req.TaskID)

		GetFile([]string{req.TaskExe, req.TaskExe})

		err := os.Chmod(Config.LocalfileDir+"/"+req.TaskExe, 0777)
		if err != nil {
			fmt.Println("os.Chmod() error")
		}

		go RunMapTask(req, d.NamenodeID)

	} else if req.TaskType == Config.Reduce {
		log.Printf("DataNode: Reduce Task %d Started!!\n", req.TaskID)

		GetFile([]string{req.TaskExe, req.TaskExe})

		err := os.Chmod(Config.LocalfileDir+"/"+req.TaskExe, 0777)
		if err != nil {
			fmt.Println("os.Chmod() error")
		}

		go RunReduceTask(req, d.NamenodeID)
	} else {
		log.Println("Invalid Task Name")
	}

	return nil
}

func (d *Datanode) SubmitTask(req string, res *[]string) error {
	//Append Map  result to per key Intermediate file

	if req == Config.Map {
		var cacheList []string

		cacheDir := Config.CacheDir
		files, _ := ioutil.ReadDir(cacheDir)

		for _, file := range files {
			fileName := file.Name()
			cacheList = append(cacheList, fileName)
		}

		*res = cacheList

	} else if req == Config.Reduce {

		resultDir := Config.ResultDir
		files, _ := ioutil.ReadDir(resultDir)


		for _, file := range files {
			var cnt = 0
			fileName := file.Name()
			PutFile([]string{"result/" + fileName, fileName}, false, &cnt, 1, true)
		}

		err := os.RemoveAll(resultDir)
		if err != nil {
			log.Println("Datanode.SubmitTask.os.RemoveAll() Error!!")
			return err
		}

		*res = nil

	} else {
		log.Println("Invalid Task Name")
	}

	fmt.Println("Datanode: Task submitted")

	return nil
}

//Scan the Map-Input Files, call Map.exe per 10-lines
func RunMapTask(req Task, namenodeID string) {

	tempFileDir := Config.LocalfileDir + "/" + Config.TempFile

	for _, fileName := range req.FileList {
		fmt.Printf("Start Map Task for File %s\n", fileName)

		//Fetch SDFSfile to local file system
		GetFile([]string{fileName, fileName})

		//Scan file
		decodedFileName := Config.DecodeFileName(fileName)
		
		data, err := os.Open(Config.LocalfileDir + "/" + decodedFileName)
		defer data.Close()

		if err != nil {
			fmt.Printf("Datanode.RunMapTask: src_file %s os.Open() error\n", decodedFileName)
			log.Println("os.Open() error")
			return
		}

		var scanner = bufio.NewScanner(data)

		var lineCnt = 0
		var buf = ""

		for scanner.Scan() {
			//Deal with EOF
			if lineCnt < 10 {
				buf += scanner.Text() + "\n"
				lineCnt++
			} else {
				// MapFunc(req.TaskExe)
				temp, err := os.Create(tempFileDir)
				if err != nil {
					fmt.Println("Datanode.RunMapTask.Scanner: os.Create() error")
					return
				}

				_, err = temp.WriteString(buf)
				if err != nil {
					fmt.Println("Datanode.RunMapTask: temp_file WriteString error")
					log.Println("temp_file WriteString error")
					return
				}

				temp.Close()

				cmd := exec.Command(Config.LocalfileDir+"/"+req.TaskExe, tempFileDir)
				res, err := cmd.Output()
				if err != nil {
					fmt.Println("Datanode.RunMapTask: exec.Command.Output Error")
					fmt.Println(err)
					return
				}

				parseMapRes(res, req.Output)

				lineCnt = 0
				buf = ""
			}
		}

		if lineCnt != 0 {
			temp, err := os.Create(tempFileDir)
			if err != nil {
				fmt.Println("os.Create() error")
				return
			}

			_, err = temp.WriteString(buf)
			if err != nil {
				fmt.Println("Datanode.RunMapTask: temp_file WriteString error")
				log.Println("temp_file WriteString error")
				return
			}

			cmd := exec.Command(Config.LocalfileDir+"/"+req.TaskExe, tempFileDir)
			res, err := cmd.Output()
			if err != nil {
				fmt.Println("Datanode.RunMapTask: exec.Command.Output Error")
				fmt.Println(err)
				return
			}

			parseMapRes(res, req.Output)
		}

		os.Remove(Config.LocalfileDir + "/" + decodedFileName)
	}

	os.Remove(tempFileDir)

	fmt.Printf("Map Task %d succeed!\n", req.TaskID)

	//When finish work, RPC namanode
	addr := Config.GetIPAddressFromID(namenodeID)
	client := NewClient(addr + ":" + Config.NamenodePort)
	client.Dial()

	var res int
	if err := client.rpcClient.Call("Namenode.SendWorkerFinishMsg", Mem.LocalID, &res); err != nil {
		fmt.Println("Datanode.RPC.Namenode.SendWorkerFinishMsg() error")
	}

	client.Close()

	return
}

func RunReduceTask(req Task, namenodeID string) {
	for _, fileName := range req.FileList {

		//Create temp file
		tempFileDir := Config.LocalfileDir + "/" + Config.TempFile
		_, err := os.Create(tempFileDir)
		if err != nil {
			fmt.Println("Datanode.RunReduceTask: os.Create() error")
			return
		}


		cacheList := req.CacheMap[fileName]

		//Create localfile/prefix_key
		localfilePath := Config.LocalfileDir + "/" + fileName
		os.Create(localfilePath)

		for _, nodeID := range cacheList {
			nodeAddr := Config.GetIPAddressFromID(nodeID)

			//Get bytes to tempfile
			var respCount int
			go RpcOperationAt("get", Config.TempFile, "cache/"+fileName, nodeAddr, Config.DatanodePort, false, &respCount, 1, false)
			<-GetFinishChan

			//Append tempfile to localfile/prefix_key
			err := Config.AppendFileToFile(tempFileDir, localfilePath)
			if err != nil {
				fmt.Println("Datanode.RunReduceTask: Append temp to localfile error")
			}
		}

		parseName := strings.Split(fileName, "_")
		if len(parseName) != 2 {
			log.Println("Parse Name Error!! Should be prefix_key")
			return
		}

		key := parseName[1]

		decodedFileName := Config.DecodeFileName(fileName)

		ReduceInputDir := Config.LocalfileDir + "/" + decodedFileName

		cmd := exec.Command(Config.LocalfileDir+"/"+req.TaskExe, ReduceInputDir)
		output, _ := cmd.Output()

		res := FormatOutput(output, key)

		err = os.Remove(localfilePath)
		if err != nil {
			fmt.Println("os.Remove error!")
		}
		os.Remove(tempFileDir)

		CacheReduceOutput(res, req.Output)
	}

	fmt.Printf("Reduce task %d finish!", req.TaskID)

	//When finish work, RPC namanode
	addr := Config.GetIPAddressFromID(namenodeID)
	client := NewClient(addr + ":" + Config.NamenodePort)
	client.Dial()

	var res int
	client.rpcClient.Call("Namenode.SendWorkerFinishMsg", Mem.LocalID, &res)

	client.Close()

	return
}

//Parse Mapper output with absolutely different valaue types e.g. {"1":["5"],"2":["1","3"],"3":["4"],"4":["2"],"5":["6"],"6":["1"]}
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
				val = append(val, s[i]) //Each value ends with '\n'
				err := CacheMapOutput(key, val, prefix)
				if err != nil {
					//panic(err)
					fmt.Println("MapperOutput error")
					return err
				}
				isKey = true
				key = key[:0]
				val = val[:0]
			} else {
				val = append(val, s[i])
			}

		}

	}

	return nil
}

//TODO: Check
func CacheMapOutput(key []byte, val []byte, prefix string) error {

	Config.CreateDirIfNotExist(Config.CacheDir)

	fileName := prefix + "_" + string(key)
	fileDir := Config.CacheDir + "/" + fileName

	file, err := os.OpenFile(fileDir, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("os.OpenFile() error")
		return err
	}
	defer file.Close()

	n, err := file.Write(val)
	if err != nil || n <= 0 {
		return err
	}

	return nil
}

func FormatOutput(output []byte, key string) string {
	res := key + ": " + string(output) + "\n"
	return res
}

//xiangl14 Todo: How to keep sdfs_dest_filename always sorted by key?
func CacheReduceOutput(res string, destFileName string) error {
	Config.CreateDirIfNotExist(Config.ResultDir)

	fileDir := Config.ResultDir + "/" + destFileName

	file, err := os.OpenFile(fileDir, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("os.OpenFile() error")
		return err
	}
	defer file.Close()

	//fmt.Println("Write to:" + fileDir)
	_, err = file.WriteString(res)

	// os.Remove(fileDir)

	return nil
}
