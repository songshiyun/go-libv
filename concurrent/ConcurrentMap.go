package concurrent

import "sync"

const defaultShardsCount = 32

type syncMap struct {
	items map[string]interface{}
	sync.RWMutex
}

type ConcurrentMap struct {
	shardCount uint32
	shards     []*syncMap
	sync.Mutex
}

func NewConcurrentMap() *ConcurrentMap {
	return NewWithShard(defaultShardsCount)
}

func NewWithShard(shardCount uint32) *ConcurrentMap {
	if !isPowerOfTwo(shardCount) {
		shardCount = defaultShardsCount
	}
	m := new(ConcurrentMap)
	m.shardCount = shardCount
	m.shards = make([]*syncMap, m.shardCount)
	for i := range m.shards {
		m.shards[i] = &syncMap{items: map[string]interface{}{}}
	}
	return m
}

func (m *ConcurrentMap) Locate(k string) *syncMap {
	return m.shards[hash(k)&uint32((m.shardCount-1))]
}

func (m *ConcurrentMap) Get(k string) (v interface{}, ok bool) {
	shard := m.Locate(k)
	shard.RLock()
	v, ok = shard.items[k]
	shard.RUnlock()
	return
}

func (m *ConcurrentMap) Put(k string, v interface{}) {
	shard := m.Locate(k)
	shard.Lock()
	shard.items[k] = v
	shard.Unlock()
}

func (m *ConcurrentMap) Remove(k string) {
	shard := m.Locate(k)
	shard.Lock()
	delete(shard.items, k)
	shard.Unlock()
}

func (m *ConcurrentMap) Has(k string) bool {
	_, ok := m.Get(k)
	return ok
}

//对单个shard加锁，当多个goroutine同时访问，其中某一下goroutine对未加锁的shard进行了修改，是否会统计不准确
//是否应该对整个ConcurrentMap加锁
func (m *ConcurrentMap) Size() int {
	size := 0
	for _, shard := range m.shards {
		shard.Lock()
		size += len(shard.items)
		shard.Unlock()
	}
	return size
}
//当多个goroutine对一个实例同时访问的时候，上面对当个shard加锁，会导致并发量高的情况下， Size()会不准确
//对整个Map加锁，but会造成一定的性能损失
func (m *ConcurrentMap) SizeAllShards() int  {
	size := 0
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	for _,shard := range m.shards {
		size += len(shard.items)
	}
	return size
}

func (m *ConcurrentMap) Flush() int {
	size := 0
	for _, shard := range m.shards {
		shard.Lock()
		size += len(shard.items)
		shard.items = make(map[string]interface{})
		shard.Unlock()
	}
	return size
}

// IterKeyWithBreakFunc is the type of the function called for each key.
//
// If false is returned,each key stops.Don't modify the ConcurrentMap in this function,or maybe leads to deadlock.
type IterKeyWithBreakFunc func(k string) bool

func (m *ConcurrentMap) EachKeyWithBreak(iter IterKeyWithBreakFunc) {
	stop := false
	for _, shard := range m.shards {
		shard.RLock()
		for k := range shard.items {
			if !iter(k) {
				stop = true
				break
			}
		}
		shard.RUnlock()
		if stop {
			break
		}
	}
}

// IterKeyFunc is the type of the function called for every key.
// Don't modify the SyncMap in this function, or maybe leads to deadlock.
type IterKeyFunc func(k string)

func (m *ConcurrentMap) EachKey(iterFun IterKeyFunc) {
	f := func(k string) bool {
		iterFun(k)
		return true
	}
	m.EachKeyWithBreak(f)
}

func (m *ConcurrentMap) IterKeys() <-chan string {
	ch := make(chan string)

	go func() {
		m.EachKey(func(k string) {
			ch <- k
		})
		close(ch)
	}()
	return ch
}

type Item struct {
	Key   string
	Value interface{}
}

type IterItemWithFunc func(item *Item) bool

func (m *ConcurrentMap) EachItemWithBreak(iter IterItemWithFunc) {
	stop := false
	for _, shard := range m.shards {
		shard.RLock()
		for k, v := range shard.items {
			if iter(&Item{k, v}) {
				stop = true
				break
			}
		}
		shard.RUnlock()
		if stop {
			break
		}
	}
}

type IterItemFunc func(item *Item)

func (m *ConcurrentMap) EachItem(iter IterItemFunc) {
	f := func(item *Item) bool {
		iter(item)
		return true
	}
	m.EachItemWithBreak(f)
}

func (m *ConcurrentMap) IterItems() <-chan Item {
	ch := make(chan Item)
	go func() {
		m.EachItem(func(item *Item) {
			ch <- *item
		})
		close(ch)
	}()
	return ch
}

func isPowerOfTwo(count uint32) bool {
	return count == 0 || (count&(count-1) == 0)
}

const hashBits = 0x7fffffff

func hash(key string) uint32 {
	var h uint32
	for _, c := range key {
		h = h*hashBits + uint32(c)
	}
	return h
}
