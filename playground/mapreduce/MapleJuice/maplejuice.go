package maplejuice

import (
	"fmt"
)

type MapperArg struct{
	maple_exe string
	num_maples int
	sdfs_intermediate_filename_prefix string
	sdfs_src_directory string
}

type ReducerArg struct{
	juice_exe string
	num_juices int
	sdfs_intermediate_filename_prefix string
	sdfs_dest_filename string
	delete_input bool
}

func RunMapper(arg []string) {
	//Check argument
	mapperArg, ok := checkMapperArg(arg)
	if !ok{
		return
	}

	mapper  := mapperArg.maple_exe
	N       := mapperArg.num_maples
	prefix  := mapperArg.sdfs_intermediate_filename_prefix
	src_dir := mapperArg.sdfs_src_directory

	//Figure out buffer size
	//TODO

	//Split all date into buffers
	//Write buffer into file before calling mapper
	//TODO

	//Wait all mappers
	//TODO

	//Return
}

func RunReducer(arg []string) {
	//Check argument
	reducerArg, ok := checkReducerArg(arg)
	if !ok{
		return
	}

	reducer      := reducerArg.juice_exe
	N            := reducerArg.num_juices
	prefix       := reducerArg.sdfs_intermediate_filename_prefix
	destfilename := reducerArg.sdfs_dest_filename
	delete_input := reducerArg.delete_input

	//Figure out buffer size
	//TODO
}

/////////////////////////////Helper functions/////////////////////////////////

func checkMapperArg(arg []string) (MapperArg, bool){
	if len(arg) < 4{
		fmt.Println("Usage: maple <maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_directory>")
		return MapperArg{}, false
	}
	return MapperArg{}, true
}

func checkReducerArg(arg []string) (ReducerArg, bool){
	if len(arg) < 5{
		fmt.Println("Usage: juice <juice_exe> <num_juices> <sdfs_intermediate_filename_prefiix> <sdfs_dest_filename> delete_input={0,1}")
		return ReducerArg{}, false
	}
	return ReducerArg{}, true
}
