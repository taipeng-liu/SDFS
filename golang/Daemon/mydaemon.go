package main

import (
	"fmt"
	"os"
	"bufio"
)

func main() {
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Println("Waiting for command...")
		cmd, _ := reader.ReadString('\n')
		fmt.Println(cmd[0:len(cmd)-1])

		if cmd == "end\n" || cmd == "end" {
			return
		}
	}
}
