package crypt

import (
	"crypto/md5"
	"fmt"
)

func Time33(string2 string)int64  {
	hash := int64(5381) // 001 010 100 000 101 ,hash后的分布更好一些
	s := fmt.Sprintf("%x", md5.Sum([]byte(string2)))
	for i := 0; i < len(s); i++ {
		hash += (hash << 5) +  int64(s[i])
	}
	hash = hash & 0x7fffffff
	return hash
}