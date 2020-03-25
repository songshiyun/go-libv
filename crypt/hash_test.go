package crypt

import (
	"fmt"
	"strconv"
	"testing"
)

func TestTime33(t *testing.T) {
	for i:=0;i<100000 ;i++  {
		fmt.Println(Time33("test_hash_"+strconv.Itoa(i)))
	}
}
