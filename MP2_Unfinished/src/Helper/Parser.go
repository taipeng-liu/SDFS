package helper

import (
	//"bufio"
	//"fmt"
	//"io"
	//"log"
	//"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

func GetMonitorList(membershipList []string, localHostName string) []string {
	var monitorList []string
	monitorIdxList := [3]int{-1, 1, 2}
	memListLen := len(membershipList)

	if memListLen >= 4 {

		for i := 0; i < memListLen; i++ {
			if strings.Contains(membershipList[i], localHostName) {
				// localIdx := i
				for _, v := range monitorIdxList {
					monitorList = append(monitorList, membershipList[(i+v+memListLen)%memListLen])
				}
				break
			}
		}
	} else {
		for i := 0; i < memListLen; i++ {
			if !strings.Contains(membershipList[i], localHostName) {
				monitorList = append(monitorList, membershipList[i])
			}
		}
	}
	return monitorList
}

func GetMonitoringList(membershipList []string, localHostName string) []string {
	var monitoringList []string
	monitorIdxList := [3]int{-2, -1, 1}
	memListLen := len(membershipList)

	if memListLen >= 4 {
		for i := 0; i < memListLen; i++ {
			if strings.Contains(membershipList[i], localHostName) {
				// localIdx := i
				for _, v := range monitorIdxList {
					monitoringList = append(monitoringList, membershipList[(i+v+memListLen)%memListLen])
				}
				break
			}
		}
	} else {
		for i := 0; i < memListLen; i++ {
			if !strings.Contains(membershipList[i], localHostName) {
				monitoringList = append(monitoringList, membershipList[i])
			}
		}
	}
	return monitoringList
}

//Call when JOIN the group
func CreateID() string {
	hostName := GetHostName()
	localTime := time.Now()
	// fmt.Println(localTime.Format(time.RFC3339))
	return hostName + ":" + localTime.Format("20060102150405")
}

func GetIPAddressFromID(ID string) string {
	return strings.Split(ID, ":")[0]
}

func GetHostName() string {
	// Get client info(host name, ID, log file name)
	cmd := exec.Command("hostname")
	hName, _ := cmd.Output()
	oldName := string(hName)
	hostName := strings.Replace(oldName, "\n", "", -1)
	return hostName
}

func GetVMNumber() int {
	// Get client info(host name, ID, log file name)
	cmd := exec.Command("hostname")
	hName, _ := cmd.Output()
	hostName := string(hName)
	var machineNO int
	// var machineName string
	if hostName[15] == '0' {
		machineNO, _ = strconv.Atoi(hostName[16:17])
	} else {
		machineNO, _ = strconv.Atoi(hostName[15:17])
	}
	return machineNO
}

func IsIntroducer() bool {
	hostName := GetHostName()
	//fmt.Print(hostName)
	//fmt.Print(IntroducerAddress)
	return hostName == IntroducerAddress
}
