package config

import (
	"os/exec"
	"strconv"
	"strings"
	"time"
	"os"
)

const (
	LocalfileDir      = "SDFS/localFile"
	SdfsfileDir       = "SDFS/sdfsFile"
	TempfileDir	  = "SDFS/tempFile"
	DatanodePort      = "8885"
	NamenodePort      = "8884"
	BLOCK_SIZE        =  512 * 1024
)

const (
	ConnHostName      = "fa19-cs425-g73-%02d.cs.illinois.edu"
	ConnType          = "udp"
	ConnPort          = "8888"
	HeartbeatPort     = "8887"
	IntroducePort     = "8886"
	ConnlocalHost     = "localhost"
	TimeOut           = 4100
	IntroducerAddress = "fa19-cs425-g73-01.cs.illinois.edu"
)

func GetLocalfilePath(localfilename string) string {
	return LocalfileDir + "/" + localfilename
}

func GetSdfsfilePath(sdfsfilename string) string {
	return SdfsfileDir + "/" + sdfsfilename
}

func CreateDirIfNotExist(dir string) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
		if err != nil{
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
