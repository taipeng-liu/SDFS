# MP3 Deisgn Docs


## Protocol Design 
```
We implement RPC client and servers in this MP. Servers include one Namenode(i.e. master) and N Datanodes (i.e. slave), in this case N = 10. 

Algorithm: User type the command in main function. Main function parse the command and call corresponding function in client.go (e.g. type command `put localfilename sdfsfilename`, main function will call `PutFile([]string{localfilename, sdfsfilename})`in client. 

How dose `PutFile` work? Firstly, client RPCs Namenode a `FindRequest`. When Namenode receive a `FindRequest`, it will figure out a list of datanodes who should save the file into `FindResponse`, and then return `FindResponse`. Then, client can get DatanodeList that contains the NodeID of datanode who should be put the file in. Client iterates the list, and in each iteration, client RPC datanode by `c.rpcClient.Call` and send file.

How dose other client functions work? Much similar to `PutFile`.

How to RPC Namenode/Datanode? We use tcp call the Namenode/Datanode with address "NamenodeAddr"+":"+Config.NamenodePort/"Datanode"+":"+Config.DatanodePort. NamenodePort and DatanodePort are consts defined in "Config/Config.go" (is currently 8885 and 8884). Struct and methods of Namenode and Datanode are all in RPC standard format which can be called by RPC client. For example, in client.go, we have `c.rpcClient.Call("Datanode.Put", putRequest, &putResponse)`, where c is a "Client" type structure, putRequest has type "PutRequest" and putResponse has type "PutResponse".

How to send a file? A file is sent by blocks. BLOCK_SIZE is a const defined in "Config/Config.go" (is currently 512). `PutRequest` has two contents: FileInfo, Block. FileInfo is the information of file, and Block is the byte data to be transferred.
```

### Definition
Detailed definition is in Code/Data Structure
Keyword: Client, Namenode, Datanode, several request/response types...

### Code Structure

#### main.go
```
Calls the following function
"Join"          -> RunDatanodeServer    at datanode.go
Not Decided     -> RunNamenodeServer    at namenode.go
"put lfn sfn"   -> PutFile              at client.go
"get sfn lfn"   -> GetFile		at client.go
"delete sfn"    -> DeleteFile		at client.go
"ls sdf"        -> ShowDatanode 	at client.go
"store"         -> ShowFile		at client.go
```

#### client.go
```
PutFile		-> Client.GetDatanodeList	-> PutFileAt 	-> Client.Put//TODO
GetFile		-> Client.GetDatanodeList	-> GetFileAt	-> Client.Get//TODO
DeleteFile	-> Client.GetDatanodeList	-> DeleteFileAt	-> Client.Delete//TODO
ShowDatanode	-> Client.GetDatanodeList 	-> print the list
ShowFile	-> listFile(Config.LocalfilePath) & listFile(Config.SdfsfilePath)

listFile
GetDatanodeList -> Namenode.GetDatanodeList	-> return DatanodeList
```

#### namenode.go
```
Namenode.GetDatanodeList //TODO

Namenode.InsertFile //TODO

RunNamenodeServer //TODO
```
#### datanode.go
```
Datanode.Put 	//TODO
Datanode.Get 	//TODO
Datanode.Delete	//TODO
Datanode.Find	//TODO

RunDatanodeServer
```
#### typeDef.go
```
Definition of Block, FileInfo, several types of request/response
```

## Data Structure && Message Type
    - added unittest into Makefile. We can individually test the functions in the all the three .cpp files. 
        Usage: ''' make unit_test '''
    - added test.sh for simple automated test.
```
Client: Addr string
	rpcClient *rpc.Client

Namenode: Filemap map[string][]string (E.g. Filemap["vm1.log"] = {"NodeID1", "NodeID2", "NodeID3"})

Datanode: NamenodeAddr string //Every datanode knows who is namenode

Block: 	Idx int
	Size int
	Content []byte

FileInfo: Filename string
	  Filesize int
	  Totalblock int

FindRequest: Filename string

FindResponse: DatanodeList []string

InsertRequest: Filename string

InsertResponse: DatanodeList []string

PutRequest: Fileinfo  FileInfo
	    Block     Block

PutResponse: Err       error
	     Statement string

GetRequest: Filename  string

GetResponse: Fileinfo  FileInfo
	     Block     Block

DeleteRequest: Filename  string

DeleteResponse: Err       error
		Statement string
```



## ToDo Follow up

### 10.26
```
TODO: Everything marked "TODO".
```


### Puzzles so far

```
When and where call RunNamenodeServer?
How to elect?

```
