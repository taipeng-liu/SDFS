package main

import (
	"fmt"
	"reflect"
	"os/exec"
)

type myinterface interface{
	Foo1()
	Foo2()
}

type mytype struct{
}

func (m mytype) Foo1() {
	fmt.Println("foo1")
}

func (m mytype) Foo2() {
	fmt.Println("foo2")
}

func main() {
	var mt mytype
	cmd := exec.Command("./reflection")
	err := cmd.Run()
}
