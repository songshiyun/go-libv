package gp

import (
	"sync"
	"sync/atomic"
	"time"
)

//http://www.zenlife.tk/goroutine-pool.md
//https://github.com/pingcap/tidb/pull/3752/files#diff-bd611d2e5d7895ddebb1d370602b49ceR94
//goroutine pool
type Pool struct {
	head        goroutine
	tail        *goroutine
	count       int
	idleTimeout time.Duration
	sync.Mutex
}

type goroutine struct {
	ch     chan func() // go func()
	next   *goroutine
	status int32
}

const (
	statusIdle  int32 = 0
	statusInUse int32 = 1
	statusDead  int32 = 2
)

func New(idleTimeout time.Duration) *Pool {
	pool := Pool{
		idleTimeout: idleTimeout,
	}
	pool.tail = &pool.head
	return &pool
}

//等价于 go func()
//能够避免 runtime.morestack()
//避免大量goroutine同时runtime.morestack()-> 1.创建的时候就分配更多的栈，2.goroutine pool池化技术
//怎么样解决goroutine泄露(最终回收)
func (pool *Pool) Go(f func()) {
	for {
		g := pool.get()
		if atomic.CompareAndSwapInt32(&g.status, statusIdle, statusInUse) {
			g.ch <- f
			return
		}
	}
}

func (pool *Pool) get() *goroutine {
	pool.Lock()
	head := &pool.head
	if head.next == nil {
		pool.Unlock()
		return pool.alloc()
	}
	ret := head.next
	head.next = ret.next
	if ret == pool.tail {
		pool.tail = head
	}
	pool.count--
	pool.Unlock()
	ret.next = nil
	return ret
}

func (pool *Pool) alloc() *goroutine {
	g := &goroutine{
		ch: make(chan func()),
	}
	go g.workLoop(pool)
	return g
}

//通过一个双向链表来管理存活的goroutine
func (g *goroutine) put(pool *Pool) {
	g.status = statusIdle
	pool.Lock()
	pool.tail.next = g
	pool.tail = g
	pool.count++
	pool.Unlock()
}

func (g *goroutine) workLoop(pool *Pool) {
	timer := time.NewTimer(pool.idleTimeout)
	for {
		select {
		case <-timer.C:
			success := atomic.CompareAndSwapInt32(&g.status, statusIdle, statusDead)
			if success {
				return
			}
		case work := <-g.ch:
			work()
			// Put g back to the pool.
			// This is the normal usage for a resource pool:
			//
			//     obj := pool.get()
			//     use(obj)
			//     pool.put(obj)
			//
			// But when goroutine is used as a resource, we can't pool.put() immediately,
			// because the resource(goroutine) maybe still in use.
			// So, put back resource is done here,  when the goroutine finish its work.
			g.put(pool)
		}
		timer.Reset(pool.idleTimeout)
	}
}
