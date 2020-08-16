package grep

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
)

const (
	connType      = "tcp"
	connPort      = "8888"
	connlocalHost = "localhost"
)

func countLine(s string) int {
	count := 0
	for _, c := range s {
		if c == '\n' {
			count++
		}
	}
	return count
}

func deleteRedundantSpaces(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func checkArg(input string) (bool, string, string) {
	format := false
	option := ""
	pattern := ""
	input = deleteRedundantSpaces(input)
	for _, c := range input {
		if c == ' ' {
			format = true
			parsedInput := strings.Split(input, " ")
			option, pattern = parsedInput[0], parsedInput[1]
		}
	}
	return format, option, pattern
}

func getFname() string {
	cmd := exec.Command("hostname")
	hname, _ := cmd.Output()
	hostname := string(hname)
	machine_no := ""
	if hostname[15] == '0' {
		machine_no = hostname[16:17]
	} else {
		machine_no = hostname[15:17]
	}
	return "../Data/vm" + machine_no + ".log"
}

func handleCommand(conn net.Conn) {
	ibuf := bufio.NewReader(conn)
	obuf := bufio.NewWriter(conn)	
	for {
		fmt.Println("Looking for command")
		//Read from client
		//ibuf := bufio.NewReader(conn)
		//obuf := bufio.NewWriter(conn)
		input, err := ibuf.ReadString('\n')
		if err != nil {
			fmt.Println("Reading Error:", err.Error())
			return
		}

		//Delete the '\n' at the end of input
		input = input[:len(input)-1]

		//Check input and get option and pattern from input
		ok, option, pattern := checkArg(input)
		if ok != true {
			fmt.Println("Bad input")
			obuf.WriteString(string("Bad input\nmp1Finished\n"))
			obuf.Flush()
			continue
			//return
		}
		fmt.Printf("Option :%s\n", option)
		fmt.Printf("Pattern: %s\n", pattern)

		//Determine the fname
		/*
			var fname string
			files, _ := ioutil.ReadDir("./")
			for _, f := range files {
				if strings.Contains(f.Name(), "vm") {
					fname = f.Name()
				}
			}
		*/
		fname := getFname()
		fmt.Printf("Filename :%s\n", fname)

		//Execute grep command
		cmd := exec.Command("grep", option, "-n", pattern, fname)
		result, err := cmd.CombinedOutput()
		if err != nil{
			fmt.Println("Command `grep` error: ", err.Error())
			obuf.WriteString("mp1Finished\n")
			obuf.Flush()
			continue
		}
		res := string(result) + "mp1Finished\n"
		//fmt.Printf("%s", res)

		//Write to the client
		_, err = obuf.WriteString(res)
		if err != nil {
			fmt.Println("WriteString error: ", err.Error())
			continue
		}
		err = obuf.Flush()
		if err != nil {
			fmt.Println("Flush error: ", err.Error())
			continue
		}
	}
	conn.Close()
}

func main() {
	fmt.Println("Start running server...")

	//Listen
	ln, lerr := net.Listen(connType, ":"+connPort)
	if lerr != nil {
		fmt.Println("Listening Error:", lerr.Error())
		os.Exit(1)
	}

	//Close the listener when the application closes
	defer ln.Close()

	fmt.Println("Listening on port: " + connPort)
	for {
		//Connect
		conn, cerr := ln.Accept()
		if cerr != nil {
			fmt.Println("Connection Error:", cerr.Error())
			os.Exit(1)
		}

		go handleCommand(conn)
	}
}
