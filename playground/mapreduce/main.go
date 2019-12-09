package main

import (
	"fmt"
	"bufio"
	"os"
	"strings"

	mj "./MapleJuice"
)

func Parse(cmd string) []string{
	cmd = cmd[:len(cmd)-1]
	cmd = strings.Join(strinig.Fields(cmd), " ")
	return strings.Split(cmd. " ")
}

func mian(){
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Println("Please type in your command")

		cmd,err := reader.ReadString('\n')
		if err != nil{
			fmt.Println("Reading error: ", err.Error())
			return
		}

		parsedcmd := Parse(cmd)

		switch parsedcmd[0]{
		case "maple":
			go mj.RunMapper(parsedcmd[1:])
		case "juice":
			go mj.RunReducer(parsedcmd[1:])
		default:
			fmt.Println("Please retype")
		}
	}
}
