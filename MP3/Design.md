# MP3 Deisgn Docs


## Protocol Design 
```
We implement RPC client and servers in this MP. Servers include one Namenode(i.e. master) and N Datanodes (i.e. slave), in this case N = 10. 

Algorithm: User type the command in main function. Main function parse the command and call corresponding function in client.go (e.g. type command `put localfilename sdfsfilename`, main function will call `PutFile([]string{localfilename, sdfsfilename})`in client. 

How dose `PutFile` work? Firstly, client RPCs Namenode a `FindRequest`. When Namenode receive a `FindRequest`, it will figure out a list of datanodes who should save the file into `FindResponse`, and then return `FindResponse`. Then, client can get DatanodeList that contains the NodeID of datanode who should be put the file in. Client iterates the list, and in each iteration, client RPC datanode by `c.rpcClient.Call` and send file.

How dose other client functions work? Much similar to `PutFile`.

How to RPC Namenode/Datanode? We use tcp call the Namenode/Datanode with address "NamenodeAddr"+":"+Config.NamenodePort/"Datanode"+":"+Config.DatanodePort. NamenodePort and DatanodePort are consts defined in "Config/Config.go" (is currently 8885 and 8884). Struct and methods of Namenode and Datanode are all in RPC standard format which can be called by RPC client. For example, in client.go, we have `c.rpcClient.Call("Datanode.Put", putRequest, &putResponse)`, where c is a "Client" type structure, putRequest has type "PutRequest" and putResponse has type "PutResponse".

How to send a file? A file is sent by blocks. BLOCK_SIZE is a const defined in "Config/Config.go" (is currently 512). `PutRequest` has two contents: FileInfo, Block. FileInfo is the payload of file, and Block is the byte data to be transferred.
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
"ls"            -> ShowDatanode 	at client.go
"store"         -> ShowFile		at client.go
```

#### client.go
```
PutFile		-> client.GetDatanodeList	-> client.Put//TODO
GetFile		-> client.GetDatanodeList	-> client.Get//TODO
DeleteFile	-> client.GetDatanodeList	-> client.Delete//TODO
ShowDatanode	-> client.GetDatanodeList 	-> print the list
ShowFile	-> listFile(Config.LocalfilePath) & listFile(Config.SdfsfilePath)

listFile
GetDatanodeList -> Namenode.GetDatanodeList	-> return DatanodeList
```

#### namenode.go
```
GetDatanodeList //TODO

RunNamenodeServer //TODO
```
#### datanode.go
```
Put 	//TODO
Get 	//TODO
Delete	//TODO
Find	//TODO

RunNamenodeServer
```
#### typeDef.go
```
Definition of Block, FileInfo, several types of request/response
```

## Data Structure && Message Type
```
Client: Addr string
	rpcClient *rpc.Client

Namenode: Filemap map[string][]string (E.g. Filemap["vm1.log"] = {"NodeID1", "NodeID2", "NodeID3"})

Datanode: (none)

Block: 	Idx int
	Size int
	Content []byte

FileInfo: Filename string
	  Filesize int
	  Totalblock int

FindRequest: Filename string

FindResponse: DatanodeList []string

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


### 10.28
```
When and where to call RunNamenodeServer? --- When RPC call GetNamenodeAddr returns "", initiates a selection 
How to elect? Simplified Bully Algorithm? --- Come Without Timer
Should we use Membership List in Failure Detect Protocol or just Full-Process List 

```

## Demo Instruction
```
Test 1: Have 10 processes (VMs) join the group, and display the membership lists at each process, both halfway along the way and finally. [4%] (same as MP2)

Have a set of files available (in local file systems or NFS) so that any client (VM) can insert them into the SDFS. Pick any dataset you want, but don’t pick files that are too small or that are too large or that contain repeated characters. (a few MBs for each file is ok). One option is to start with the datasets at the web-caching.com website: http://www.web-caching.com/traces-logs.html. For testing purposes, you could also generate your own files by using (file size will be count megabytes) dd if=/dev/urandom of=filename bs=1024*1024 count=2 

Test 2: Insert several files (from any server)--at least 10 files should be inserted--and immediately after the write is completed, read one file (file and reader process selected randomly by the TA, reader should be different from writer). Then display the file using more/less and compare the file sizes between the local copy (from which it was initially inserted)  and the SDFS-fetched copy. [8%]

Test 3: Show (by using the “ls sdfsfilename” command, and “store” command at all processes) that the files above (a few randomly selected by TAs) are replicated exactly the right number of times in the cluster. [4%]

Test 4: TA will pick a (small) file (different from Test 2) and ask you to insert, and then fail (Ctrl-C or kill, not leave) two of its replicas. Then do a “get” on the same file, and display the file using more/less and compare the file sizes of the local copy and SDFS copy. [4%]

Test 5: After a while, you need to show (via the “ls sdfsfilename” command, and the “store” command) that the file above has been re-replicated according to the replication factor that you chose to tolerate up to 3 failures. [4%].

Test 6: The TA will pick two (small) files and ask you to "put" them from different machines (picked randomly by the TA) with the same "sdfsfilename.foo.*" The two writes will be seconds apart from each other. The second put should raise a confirmation prompt. The TA will ask you to reject or ignore the second update using this prompt. [8%]

Test 7: Delete a file by using “delete sdfsfilename”, and from a different client (picked randomly by TA) get that same file (it should output something like “the file is not available” ).[8%]
```