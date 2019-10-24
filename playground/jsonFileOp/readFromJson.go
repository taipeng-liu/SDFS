package read

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
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
	jsonFile, err := os.Open("users.json")

	if err != nil {
		fmt.Println(err)
	}
	
	fmt.Println("Open users.json")

	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	var users Users

	json.Unmarshal(byteValue, &users)
	fmt.Println(users)

	for _, user := range users.Users {
		fmt.Println(user.Type)
		fmt.Println(strconv.Itoa(user.Age))
		fmt.Println(user.Name)
		fmt.Println(user.Social.Facebook)
	}
}
