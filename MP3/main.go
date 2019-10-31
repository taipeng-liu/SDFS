package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	Config "./Config"
	Mem "./Membership"
	Sdfs "./SDFS"
)

func Parse(cmd string) []string {
	cmd = cmd[:len(cmd)-1]
	cmd = strings.Join(strings.Fields(cmd), " ")
	return strings.Split(cmd, " ")
}

func main() {
	vmNumber := Config.GetVMNumber()
	logFile, err := os.OpenFile("MP3_"+strconv.Itoa(vmNumber)+".log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer logFile.Close()

	log.SetOutput(logFile)

	isIntroducer := Config.IsIntroducer()

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Println("Please type your command:")
		cmd, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Reading error: ", err.Error())
			return
		}

		parsedcmd := Parse(cmd)

		switch parsedcmd[0] {
		case "join":
			log.Println("Main: Join the group")
			go Mem.RunNode(isIntroducer)
			go Sdfs.RunDatanodeServer() //"SDFS/DatanodeServer.go"
			go Sdfs.RunNamenodeServer()
			//TODO Decide when and where run Namenode Server???
		case "leave":
			log.Println("Main: Leave the group")
			go Mem.StopNode()
		case "mlist":
			log.Println("Main: Show the current Membership List")
			go Mem.ShowList()
		case "ID":
			log.Println("Main: Show the current Node ID")
			go Mem.ShowID()
		case "put":
			log.Println("Main: Put localfilename sdfsfilename")
			go Sdfs.PutFile(parsedcmd[1:], true) //"SDFS/client.go"
		case "get":
			log.Println("Main: Get sdfsfilename localfilename")
			go Sdfs.GetFile(parsedcmd[1:], true) //"SDFS/client.go"
		case "delete":
			log.Println("Main: Delete sdfsfile")
			go Sdfs.DeleteFile(parsedcmd[1:]) //"SDFS/client.go"
		case "ls":
			log.Println("Main: List all servers who save the file")
			go Sdfs.ShowDatanode(parsedcmd[1:]) //"SDFS/client.go"
		case "store":
			log.Println("Main: Show all files")
			go Sdfs.ShowFile() //"SDFS/client.go"
		case "clear":
			log.Println("Main: clear directory sdfsFile")
			go Sdfs.Clear() //"SDFS/client.go"
		case "y":
			Sdfs.YESorNO <- true
			Sdfs.KillTimeOut30s <- ""
		case "n":
			Sdfs.YESorNO <- false
			Sdfs.KillTimeOut30s <- ""
		default:
			log.Println("Main: Don't support this command")
		}
	}
}
