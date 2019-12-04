package maplejuice

import (
)

type MapReduce interface{
	Map(string) map[string]string
	Reduce(map[string]string) map[string]string 
}
