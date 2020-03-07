package concurrent

import (
	"sort"
	"strconv"
	"testing"
	"time"
)

func Test_New(t *testing.T) {
	m1 := NewConcurrentMap()
	if m1 == nil {
		t.Error("NewConcurrentMap() map fail")
	}
	if m1.shardCount != defaultShardsCount {
		t.Error("new() map shard count is wrong")
	}
	if m1.Size() != 0 {
		t.Error("NewConcurrentMap() Map is wrong ,Empty ")
	}

	var count uint32 = 64
	m2 := NewWithShard(count)
	if m2 == nil {
		t.Error("NewWithShard with count fail")
	}
	t.Log("m2's Len : ", m2.shardCount)
	if m2.shardCount != count {
		t.Error("NewWithShard with count is Not Equal")
	}

	t.Log("NewWithShard with count ,Size is : ", m2.Size())
}

func Test_Set(t *testing.T) {
	m := NewConcurrentMap()
	m.Put("name", "songshiyun")
	m.Put("age", "24")
	if m.Size() != 2 {
		t.Error("map should have two item")
	}
	t.Log("Map is : ", &m)
}

func Test_Get(t *testing.T) {
	m := NewConcurrentMap()
	v, ok := m.Get("not_exit")
	if ok {
		t.Error("the key should not be exist")
	}
	if v != nil {
		t.Error("the value should be nil")
	}

	m.Put("name", "songshiyun")
	v1, ok1 := m.Get("name")
	if !ok1 {
		t.Error("ok1 should be ok")
	}
	t.Log("the value is :", v1)
}

func TestConcurrentMap_Has(t *testing.T) {
	m := NewConcurrentMap()
	bol := m.Has("not_exit")
	if bol {
		t.Error("the key dose not exist")
	}
	m.Put("name", "songshiyun")
	bol2 := m.Has("name")
	if !bol2 {
		t.Error("the key <name> should be exist")
	}
}

func TestConcurrentMap_Flush(t *testing.T) {
	var count uint32 = 64
	m := NewWithShard(count)
	for i := 0; i < 42; i++ {
		m.Put(strconv.Itoa(i), i)
	}
	fl := m.Flush()
	if fl != 42 {
		t.Error("Flush should return the size before remove")
	}
	if m.Size() != 0 {
		t.Error("After Flush ,Size should be Zero")
	}
	if m.shardCount != count {
		t.Error("After Flush, thi shardCount should be immutable")
	}
}

func TestConcurrentMap_IterKeys(t *testing.T) {
	loops := 100
	keys := make([]string, loops)

	m := NewConcurrentMap()
	for i := 0; i < loops; i++ {
		k := strconv.Itoa(i)
		keys[i] = k
		m.Put(k, i)
	}

	kk := make([]string, 0)
	for key := range m.IterKeys() {
		kk = append(kk, key)
	}

	if len(keys) != len(kk) {
		t.Error("IterKeys does not loop the right times")

	}
	sort.Strings(kk)
	sort.Strings(keys)

	for i, v := range kk {
		if v != keys[i] {
			t.Error("it should bt equal, bad loop times")
		}
	}
}

//这种测试方式好像无法模拟
/**
新建一个simple应用
package main

import (
	"concurrentMap"
	"fmt"
	"strconv"
	"time"
)

func main() {
	loops := 100
	m := concurrentMap.NewConcurrentMap()
	go func() {
		for i := 0; i < loops; i++ {
			k := strconv.Itoa(i)
			m.Put(k, i)
		}
	}()
	go func() {
		size1 := m.SizeAllShards()
		fmt.Println("Goroutine1,Get Size With all Shards: ",size1)
		size2:=m.Size()
		fmt.Println("Goroutine1,Get Size: ",size2)
	}()
	go func() {
		for i := 100; i < loops*100; i++ {
			k := strconv.Itoa(i)
			m.Put(k, i)
		}
	}()
	go func() {
		size3 := m.SizeAllShards()
		fmt.Println("Goroutine2,Get Size With all Shards: ",size3)
		size4:=m.Size()
		fmt.Println("Goroutine4,Get Size: ",size4)
	}()
	time.Sleep(100*time.Second)
}

输出:
Goroutine1,Get Size With all Shards:  388
Goroutine2,Get Size With all Shards:  587
Goroutine4,Get Size:  840
Goroutine1,Get Size:  891
并发Size()并不准确
 */
func TestConcurrentMap_SizeAllShards(t *testing.T) {
	loops := 100

	m := NewConcurrentMap()
	go func() {
		for i := 0; i < loops; i++ {
			k := strconv.Itoa(i)
			m.Put(k, i)
		}
	}()
	go func() {
		size1 := m.SizeAllShards()
		t.Log("Goroutine1,Get Size With all Shards: ",size1)
		size2:=m.Size()
		t.Log("Goroutine1,Get Size: ",size2)
	}()
	go func() {
		for i := 100; i < loops*100; i++ {
			k := strconv.Itoa(i)
			m.Put(k, i)
		}
	}()
	go func() {
		size3 := m.SizeAllShards()
		t.Log("Goroutine2,Get Size With all Shards: ",size3)
		size4:=m.Size()
		t.Log("Goroutine4,Get Size: ",size4)
	}()
	time.Sleep(10*time.Second)
}