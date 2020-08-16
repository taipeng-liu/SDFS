package helper

import (
	"encoding/json"
	"os"
)

func WriteIntMapToJson(mymap map[string]int, prefix string) error {
	for key, val := range mymap {
		filebyte, _ := json.MarshalIndent(map[string]int{key: val}, "", " ")

		file, err := os.OpenFile(prefix+"_"+key, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
		defer file.Close()

		if err != nil {
			return err
		}

		if _, err := file.Write(filebyte); err != nil {
			return err
		}
	}

	return nil
}

func WriteStringSliceMapToJson(mymap map[string][]string, prefix string) error {
	for key, val := range mymap {
		filebyte, _ := json.MarshalIndent(map[string][]string{key: val}, "", " ")

		file, err := os.OpenFile(prefix+"_"+key, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
		defer file.Close()

		if err != nil {
			return err
		}

		if _, err := file.Write(filebyte); err != nil {
			return err
		}
	}

	return nil
}
