package main

import (
	"os/exec"
	"fmt"
)

func main() {
	cmd := exec.Command("./WordCountMapper", "test")
	res, _ := cmd.Output()
	fmt.Printf("%s\n", res)
}
