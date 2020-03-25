package pool

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"
)

type Pool struct {
	mu 			sync.Mutex
	freeConn 	[]*poolConn
	numOpen 	int
	openerCh 	chan struct{}
	closed 		bool
	maxIdle 	int
	maxOpen 	int
	address 	string
	stop func()
}

type poolConn struct {
	pool 		*Pool
	createdAt 	time.Time
	conn 		net.Conn
	inUse 		bool
	closed 		bool
}

func NewPool(maxOpen,maxIdle int,address string) *Pool {
	ctx,cancel := context.WithCancel(context.Background())
	pool := &Pool{
		openerCh:make(chan struct{},1000),
		stop:cancel,
		address:address,
		maxIdle:maxIdle,
		maxOpen:maxOpen,
	}
	go pool.connOpener(ctx)
	return pool
}

func (pool *Pool)Acquire(ctx context.Context) (*poolConn,error) {
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		return nil,errors.New("pool is closed")
	}
	select {
	default:
	case <-ctx.Done():
		pool.mu.Unlock()
		return nil,ctx.Err()
	}
	numFree := len(pool.freeConn)
	if numFree > 0 {
		 conn := pool.freeConn[0]
		 pool.freeConn = pool.freeConn[:numFree-1]
		 conn.inUse = true
		 pool.mu.Unlock()
		 return conn,nil
	}
	if pool.maxOpen > 0 && pool.numOpen >= pool.maxOpen {
		//这里可以采用channel,一直阻塞，等待有可用的链接
		return nil,errors.New("no conn")
	}
	pool.numOpen++
	pool.mu.Unlock()

	ci,err := net.Dial("TCP",pool.address)
	if err != nil {
		pool.mu.Lock()
		pool.numOpen--
		pool.mu.Unlock()
		return nil,err
	}

	pool.mu.Lock()
	pc := &poolConn{
		pool:pool,
		createdAt:time.Now(),
		conn:ci,
		inUse:true,
	}
	pool.mu.Unlock()
	return pc,nil
}


func (pool *Pool)connOpener(ctx context.Context)  {
	for  {
		select {
		case <-ctx.Done():
			return
		case <-pool.openerCh:
			pool.openNewConnection(ctx)
		}
	}
}

func (pool *Pool)openNewConnection(ctx context.Context)  {
	conn,err := net.Dial("TCP", pool.address)
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.closed {
		if err != nil {
			conn.Close()
		}
		return
	}
	pc := &poolConn{
		pool:pool,
		createdAt:time.Now(),
		conn:conn,
	}
	if ! pool.putConnPutLocked(pc) {
		pool.numOpen--
		conn.Close()
	}
}

func (pool *Pool)putConnPutLocked(pc *poolConn) bool {
	if pool.closed {
		return false
	}
	if pool.maxOpen > 0 && pool.numOpen > pool.maxOpen {
		return false
	}
	if pool.maxIdleConnLocked() > len(pool.freeConn) {
		pool.freeConn = append(pool.freeConn,pc)
		pool.numOpen++
		return true
	}
	return false
}

func (pool *Pool)maxIdleConnLocked() int{
	n := pool.maxIdle
	switch  {
	case n==0:
		return 2
	case n < 0:
		return 0
	default:
		return n
	}
}