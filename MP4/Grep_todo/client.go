package grep

import (
	"bufio"
	"fmt"
	"strconv"
	"log"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
)

type logPackage struct {
	logFile   string
	numOfLine int
}

const (
	connHostName  = "fa19-cs425-g73-%02d.cs.illinois.edu"
	testName      = "fa19-cs425-g73-02.cs.illinois.edu"
	connType      = "tcp"
	connPort      = "8888"
	connlocalHost = "localhost"
	timeOut       = 10
	serverNum = 6
	unitTestNum = 3
)



func listen(logger *log.Logger, conn net.Conn, vmID int) (bool, int) {
	//Message from server
	var lineCount = 0 
	logReader := bufio.NewReader(conn)
	// Read String from Server
	for {
		logRecord, err := logReader.ReadString('\n')
		if err != nil {
			if err == io.EOF{
				fmt.Printf("VM%02d:Server Crashes or Conn Error\n", vmID)
				return false, 0
			} else{
				os.Exit(2)
			}
		}
		if logRecord == "mp1Finished\n"{
			break
		}
		lineCount ++
		logger.Printf("VM%02d: %s\n", vmID, logRecord)		
		//fmt.Printf("VM%02d: %s\n", vmID, logRecord)		
	}

	return true, lineCount
}

func deleteRedundantSpaces(s string) string {
        return strings.Join(strings.Fields(s), " ")
}


func getClientInfo() (int, string) {
	// Get client info(host name, ID, log file name)
	cmd := exec.Command("hostname")
	hName,_ := cmd.Output()
	hostName := string(hName)
	var machineNO int
	var machineName string
	if hostName[15] == '0'{
		machineNO, _  = strconv.Atoi(hostName[16:17])
		machineName = "../Data/vm" + hostName[16:17] +".log" 
	} else {
		machineNO, _ = strconv.Atoi(hostName[15:17])
		machineName = "../Data/vm" + hostName[15:17] +".log"
	}
	return machineNO, machineName
}


func checkArg(input string) (bool, string, string) {
        inputCheck := false
        option := ""
        pattern := ""
        input = deleteRedundantSpaces(input)
        for _, c := range input {
                if c == ' ' {
                        inputCheck = true
                        parsed_input := strings.Split(input, " ")
                        option, pattern = parsed_input[0], parsed_input[1]
                }
        }
        return inputCheck, option, pattern
}



func dealClient(myLogger *log.Logger, cmd string, vmID int, fName string) (bool, int) {
	inputCheck, option, pattern := checkArg(cmd)
	if inputCheck != true{
		fmt.Print("Bad Input\n")
		return false, 0
	}
	myCmd := exec.Command("grep", option, "-n", pattern, fName)
	result, _ := myCmd.CombinedOutput()
	res := string(result)
	
	//fmt.Print(res)	
	
	var lineCount = 0
	for _,c := range result {
		if c == '\n'{
			lineCount++
		}
	}	
        myLogger.Printf("VM%02d: %s\n", vmID, res)
	return true, lineCount
	
}


func main() {
	f, fileErr := os.OpenFile("MP1.log",os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if fileErr != nil {
		log.Println(fileErr)
	}
	defer f.Close()
	myLogger := log.New(f, "==== Log File for MP1 : ", log.Ldate|log.Ltime|log.Lshortfile)
	conn := make([]net.Conn, serverNum)
	connErr := make([]error, serverNum)
	//Connect to server
	
	clientVMID, clientFileName := getClientInfo()
	
	for i := 0; i < serverNum; i++ {
		// If it's client itself, use local grep instead of TCP 
		if i + 1 == clientVMID {
			continue
		}
		
		//Set Hostname for connection
		vmIPAddress := fmt.Sprintf(connHostName, i+1)
		
		//Connect to VM
		conn[i], connErr[i] = net.Dial(connType, vmIPAddress+":"+connPort)
		//Check error
		if connErr[i] != nil {
			conn[i] = nil
			//conn[i], connErr[i] = net.Dial(connType, vmIPAddress+":"+connPort)
			fmt.Println("==== Dial Error:", connErr[i].Error(), " Can't connect to: ", vmIPAddress, "====")
		}
		// Defer connection Close
		if conn[i] != nil{
			defer conn[i].Close()
		}
	}

	//Read from stdin
	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Println("Please Type Command : ")
		cmd, _ := reader.ReadString('\n')
		
		for i := 0; i < serverNum; i++ {
			if i + 1 == clientVMID {
				cmdOK, lineCount := dealClient(myLogger, cmd, i + 1, clientFileName)
       				if cmdOK == true {  
					fmt.Printf("VM%02d: The line count from VM No.%02d is: %d\n", i + 1, i + 1, lineCount)
				}
			}
			if conn[i] == nil || connErr[i] != nil{
				continue
			} 
			fmt.Fprintf(conn[i], cmd)

			//Listen from server
			serverStatus, lineCount := listen(myLogger, conn[i], i + 1)
			
			if serverStatus == false {
				conn[i].Close()
				conn[i] = nil
			} else {
				fmt.Printf("VM%02d: The line count from VM No.%02d is: %d\n", i + 1, i + 1, lineCount)		
			}		
		}
	}
	
}
