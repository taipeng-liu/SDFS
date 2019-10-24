package main

import (
	"encoding/json"
	"io/ioutil"
)

type Users struct {
	Users []User `json:"users"`
}

type User struct {
	Name string `json:"name"`
	Type string `json:"type"`
	Age  int    `json:"Age"`
	Social Social `json:"social"`
}

type Social struct {
	Facebook string `json:"facebook"`
	Twitter  string `json:"twitter"`
}

func main() {
	a := User{"Taipeng","Student",22,Social{"facebook/taipeng","twitter/taipeng"}}
	userList := Users{[]User{a}}

	file, _ := json.MarshalIndent(userList, "", " ")

	_ = ioutil.WriteFile("users.json",file, 0644)
}
