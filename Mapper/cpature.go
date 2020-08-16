package main

import (
	"fmt"
	"os"
	"os/exec"
)

func parseMapRes(res []byte, prefix string) error {
	s := string(res)

	isKey := true

	var key, val []byte

	for i := 0; i < len(s); i++ {
		if isKey {
			if s[i] == ':' {
				isKey = false
			} else {
				key = append(key, s[i])
			}
		} else {
			if s[i] == '\n' {
				err := WriteFile(key, val, prefix)
				if err != nil {
					panic(err)
					return err
				}
				isKey = true
				key = key[:0]
				val = val[:0]
			} else {
				val = append(val, s[i])
			}

		}

	}
	// var reader = strings.NewReader(s)

	// for reader.Read() {
	// 	if
	// }

	return nil
}

func WriteFile(key []byte, val []byte, prefix string) error {
	fileName := prefix + "_" + string(key)
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)

	n, err := file.Write(val)
	fmt.Println(n)
	if err != nil || n <= 0 {
		return err
	}

	return nil

}

func main() {
	temp := "./webTest"
	cmd := exec.Command("./WebMapper", temp)
	res, _ := cmd.Output()
	//s := string(res)
	//fmt.Println(s)
	parseMapRes(res, "prefix")

}
