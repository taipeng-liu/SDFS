package config

import (
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
	"fmt"
)

const (
	LocalfileDir = "SDFS/localFile"
	SdfsfileDir  = "SDFS/sdfsFile"
	TempfileDir  = "SDFS/tempFile"
	DatanodePort = "8885"
	NamenodePort = "8884"
	ReplicaNum   = 3
	BLOCK_SIZE   = 10 * 1024 * 1024
	LINENUM      = 10
	TempFile     = "temp"
	CacheDir     = "SDFS/sdfsFile/cache"
	ResultDir    = "SDFS/localFile/result"
	Map          = "map"
	Reduce       = "reduce"
)

const (
	ConnType          = "udp"
	ConnPort          = "8888"
	HeartbeatPort     = "8887"
	IntroducePort     = "8886"
	ConnlocalHost     = "localhost"
	TimeOut           = 4100
	IntroducerAddress = "fa19-cs425-g73-01.cs.illinois.edu"
)

func AppendFileToFile(src_file string, dest_file string) error {
	dest_fd, err := os.OpenFile(dest_file, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Println("AppendFileToFile: os.OpenFile() error")
		return err
	}
	defer dest_fd.Close()

	src_byte, err := ioutil.ReadFile(src_file)
	if err != nil {
		log.Println("AppendFileToFile: ioutil.ReadFile() error")
		return err
	}

	if _, err := dest_fd.Write(src_byte); err != nil {
		log.Println("AppendFileToFile: fd.Write() error")
		return err
	}
	return nil
}

func ParseString(cmd string) []string {
	cmd = strings.Join(strings.Fields(cmd), " ")
	return strings.Split(cmd, " ")
}

func GetLocalfilePath(localfilename string) string {
	return LocalfileDir + "/" + localfilename
}

func GetSdfsfilePath(sdfsfilename string) string {
	return SdfsfileDir + "/" + sdfsfilename
}

func CreateDirIfNotExist(dir string) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			log.Println(err)
		}
	}
}

func CreateID() string {
	hostName := GetHostName()
	localTime := time.Now()
	return hostName + ":" + localTime.Format("20060102150405")
}

func GetIPAddressFromID(ID string) string {
	return strings.Split(ID, ":")[0]
}

func GetHostName() string {
	cmd := exec.Command("hostname")
	hName, _ := cmd.Output()
	oldName := string(hName)
	hostName := strings.Replace(oldName, "\n", "", -1)
	return hostName
}

func GetVMNumber() int {
	cmd := exec.Command("hostname")
	hName, _ := cmd.Output()
	hostName := string(hName)
	var machineNO int
	if hostName[15] == '0' {
		machineNO, _ = strconv.Atoi(hostName[16:17])
	} else {
		machineNO, _ = strconv.Atoi(hostName[15:17])
	}
	return machineNO
}

func IsIntroducer() bool {
	hostName := GetHostName()
	return hostName == IntroducerAddress
}

func TimeCount() func() {
	start := time.Now()

	return func() {
		cost := time.Since(start)
		fmt.Printf("Time cost: %v\n", cost)
	}
}

func EncodeFileName(src string) string {
	res := strings.ReplaceAll(src, "/", "***")
	return res
}

func DecodeFileName(src string) string {
	res := strings.ReplaceAll(src, "***", "/")
	return res
}

func Min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func ParseDir(str string) []string {
	idx := strings.LastIndex(str, "/")
	dirName := str[:idx]
	fieName := str[idx+1 : len(str)]
	return []string{dirName, fieName}
}

func Hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
