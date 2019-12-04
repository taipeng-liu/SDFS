package sdfs

import (
	"time"
)

type MapperArg struct {
	Maple_exe                         string
	Num_maples                        int
	Sdfs_intermediate_filename_prefix string
	Sdfs_src_directory                string
}

type ReducerArg struct {
	Juice_exe                         string
	Num_juices                        int
	Sdfs_intermediate_filename_prefix string
	Sdfs_dest_filename                string
	Delete_input                      bool
}

type Task struct {
	TaskID    int
	TaskType  string //"map" or "reduce"
	TaskExe   string //"WordCountMap" or "WordCountReducer"
	StartTime time.Time
	FileList  []string //Note: filename is decoded, e.g. "MyDirName/MyFileName"
	Output    string
}

type FindRequest struct {
	Filename string
}

type FindResponse struct {
	DatanodeList []string
}

type InsertRequest struct {
	Filename string
	NodeID   string
}

type InsertResponse struct {
	DatanodeList []string
}

type PermissionRequest struct {
	Filename  string
	MustWrite bool
}

type ReReplicaRequest struct {
	Filename     string
	DatanodeList []string
}

type PutRequest struct {
	Filename string
	Eof      bool
	Offset   int64
	Content  []byte
	Hostname string
	AppendMode bool
}

type PutResponse struct {
	Response string
}

type AppendRequest struct {
	Filename string
	Content  []byte
}

type GetRequest struct {
	Filename string
	Offset   int64
	Size     int
}

type GetResponse struct {
	Eof     bool
	Content []byte
}

type DeleteRequest struct {
	Filename string
}

type DeleteResponse struct {
	Err       error
	Statement string
}
