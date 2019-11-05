# MP3 Simple Distributed File(SDFS) System 


## Demo Use

### Simply `go run main.go` , run the following commands
1. `join`:  Join the group 
2. `leave`: Leave current group
3. `ID`: Show local ID at current group
4. `mlist`: Show current membership list of the group
5.  `put`: localfilename sdfsfilename: Write(Insert or Update) localfilename distributedly
6. `get`： sdfsfilename localfilename: Fetches sdfsfilename to local 
7. `delete`: sdfsfilename: Delete all replicas of sdfsfilename
8. `ls`: sdfsfilename: List all VMs who save the file sdfsfilename
9. `store`: Show all sdfs files stored in this VM

## Data Structure && Message Type
```
Client: 
	Addr string
	rpcClient *rpc.Client

Namenode: 
	Filemap map[string]*FileMetadata  //Key:sdfsFilename  Value:Pointer of metadata
	Nodemap map[string][]string 	  //Key:NodeID        Value:sdfs FileList

FileMetadata 
	DatanodeList []string // All the VMs who store this sdfsfile
	LastWrtTime  time.Time // Last put time of this sdfsfile  

Datanode:
	NamenodeID string   //NodeID, not Address
	FileList   []string //list of sdfsfile

Block: 	Idx int
	Size int
	Content []byte

FileInfo: 
	Filename string
	Filesize int
	Totalblock int

```
