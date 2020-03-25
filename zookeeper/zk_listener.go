package zookeeper

import (
	"path"
	"sync"
	"time"

	perrors "github.com/pkg/errors"
	"github.com/samuel/go-zookeeper/zk"
	log "github.com/sirupsen/logrus"
	_"github.com/songshiyun/go-libv/log"
)


const (
	ConnDelay    = 3
	MaxFailTimes = 15
)


type ZkEventListener struct {
	client *ZookeeperClient
	pathMapLock sync.Mutex
	pathMap map[string]struct{}
	wg sync.WaitGroup
}

func NewZKListenner(client *ZookeeperClient) *ZkEventListener  {
	return &ZkEventListener{
		client:client,
		pathMap:make(map[string]struct{}),
	}
}
func (l *ZkEventListener)SetClient(client *ZookeeperClient)  {
	l.client = client
}

func (l *ZkEventListener)ListenNodeEvent(path string,listenners ...DataListener)bool {
	l.wg.Add(1)
	defer l.wg.Done()
	var event zk.Event
	for  {
		keyEventCh, err := l.client.ExistW(path)
		if err != nil {
			return false
		}
		select {
		case event = <- keyEventCh:
			log.WithFields(log.Fields{
				"client":l.client.name,"type":event.Type,"server":event.Server,"path":event.Path,"state":StateToString(event.State),"error":event.Err,
			}).Info("get a zookeeper event")
			switch event.Type {
			case zk.EventNodeDataChanged:
				log.Infof("zk.ExistW(key{%s}) = event{EventNodeDataChanged}", path)
				if len(listenners) >0 {
					content,_,_ := l.client.Conn.Get(event.Path)
					listenners[0].DataChange(Event{
						Path:event.Path,
						Action:EventTypeUpdate,
						Content:string(content),
					})
				}
			case zk.EventNodeCreated:
				log.Warnf("zk.ExistW(key{%s}) = event{EventNodeCreated}", path)
				if len(listenners) > 0 {
					content, _, _ := l.client.Conn.Get(event.Path)
					listenners[0].DataChange(Event{Path: event.Path, Action: EventTypeAdd, Content: string(content)})
				}
			case zk.EventNotWatching:
				log.Warnf("zk.ExistW(key{%s}) = event{EventNotWatching}", path)
			case zk.EventNodeDeleted:
				log.Warnf("zk.ExistW(key{%s}) = event{EventNotWatching}", path)
			}
			return true //todo
		case <- l.client.Done():
			return false
		}
	}
}

func (l *ZkEventListener)ListenServiceEvent(zkPath string,listener DataListener)  {
	var (
		err       error
		watchPath string
		children  []string
	)
	l.pathMapLock.Lock()
	_,ok := l.pathMap[zkPath]
	l.pathMapLock.Unlock()
	if ok {
		log.Warnf("@zkPath %s has already been listened.", zkPath)
		return
	}
	l.pathMapLock.Lock()
	l.pathMap[zkPath] = struct{}{}
	l.pathMapLock.Unlock()
	log.Infof("listen zookeeper path{%s} event and wait to get all zk nodes", zkPath)
	children, err = l.client.GetChildren(zkPath)
	if err != nil {
		children = nil
		log.Warnf("fail to get children of zk path{%s}", zkPath)
	}
	for _,childNode := range children {
		watchPath = path.Join(zkPath,childNode)
		content, _, err := l.client.Conn.Get(watchPath)
		if err != nil {
			log.Errorf("Get new node path {%v} 's content error,message is  {%v}", watchPath, perrors.WithStack(err))
		}
		if !listener.DataChange(Event{Path: watchPath, Action: EventTypeAdd, Content: string(content)}) {
			continue
		}
		log.Infof("listen zookeeper service key{%s}", watchPath)
		go func(zkPath string, listener DataListener) {
			if l.ListenNodeEvent(zkPath) {
				listener.DataChange(Event{Path: zkPath, Action: EventTypeDel})
			}
			log.Warnf("listenSelf(zk path{%s}) goroutine exit now", zkPath)
		}(watchPath, listener)
	}
	log.Infof("listen zookeeper path{%s}", zkPath)
	go func(zkPath string, listener DataListener) {
		l.listenDirEvent(zkPath, listener)
		log.Warnf("listenDirEvent(zkPath{%s}) goroutine exit now", zkPath)
	}(zkPath, listener)
}

func (l *ZkEventListener)listenDirEvent(zkPath string,listener DataListener) {
	l.wg.Add(1)
	defer l.wg.Done()
	var (
		failTimes int
		event     chan struct{}
		zkEvent   zk.Event
	)
	event = make(chan struct{})
	defer close(event)
	for  {
		children, eventCh, err := l.client.GetChildrenW(zkPath)
		if err != nil {
			failTimes++
			if MaxFailTimes <= failTimes {
				failTimes = MaxFailTimes
			}
			log.Warnf("listenDirEvent(path{%s}) = error{%v}", zkPath, err)
			// clear the event channel
		CLEAR:
			for {
				select {
				case <-event:
				default:
					break CLEAR
				}
			}
			l.client.RegisterEvent(zkPath,&event)
			select {
			case <-time.After(timeSecondDuration(failTimes * ConnDelay)): //todo学习timeWheel时间轮实现
				l.client.UnregisterEvent(zkPath,&event)
				continue
			case <-l.client.Done():
				l.client.UnregisterEvent(zkPath, &event)
				log.Warnf("client.done(), listen(path{%s}) goroutine exit now...", zkPath)
				return
			case <-event:
				log.Infof("get zk.EventNodeDataChange notify event")
				l.client.UnregisterEvent(zkPath, &event)
				l.handleZkNodeEvent(zkPath, nil, listener)
				continue
			}
		}
		failTimes = 0
		for _,child := range children{
			watchPath := path.Join(zkPath,child)
			l.pathMapLock.Lock()
			_, ok := l.pathMap[watchPath]
			l.pathMapLock.Unlock()
			if ok {
				log.Warnf("@zkPath %s has already been listened.", zkPath)
				continue
			}
			l.pathMapLock.Lock()
			l.pathMap[watchPath] = struct{}{}
			l.pathMapLock.Unlock()
			content,_,err := l.client.Conn.Get(watchPath)
			if err != nil {
				log.Errorf("Get new node path {%v} 's content error,message is  {%v}", watchPath, perrors.WithStack(err))
			}
			log.Infof("get new node path {%v} 's content %s ",zkPath,string(content))

			if !listener.DataChange(Event{Path:watchPath,Action:EventTypeAdd,Content:string(content)}) {
				continue
			}
			log.Infof("listen zk service key{%s}", watchPath)
			go func(zkPath string, listener DataListener) {
				if l.ListenNodeEvent(zkPath) {
					listener.DataChange(Event{Path: zkPath, Action: EventTypeDel})
				}
				log.Warnf("listenSelf(zk path{%s}) goroutine exit now", zkPath)
			}(watchPath, listener)
			//listen sub path recursive
			go func(zkPath string, listener DataListener) {
				l.listenDirEvent(zkPath, listener)
				log.Warnf("listenDirEvent(zkPath{%s}) goroutine exit now", zkPath)
			}(watchPath, listener)
		}
		select {
		case zkEvent = <- eventCh:
			log.Warnf("get a zookeeper zkEvent{type:%s, server:%s, path:%s, state:%d-%s, err:%s}",
				zkEvent.Type.String(), zkEvent.Server, zkEvent.Path, zkEvent.State, StateToString(zkEvent.State), zkEvent.Err)
			if zkEvent.Type != zk.EventNodeChildrenChanged {
				continue
			}
			l.handleZkNodeEvent(zkEvent.Path,children,listener)
		case <-l.client.Done():
			log.Warnf("client.done(), listen(path{%s}) goroutine exit now...", zkPath)
			return
		}
	}

}

func (l *ZkEventListener)handleZkNodeEvent(zkPath string, children []string, listener DataListener)  {
	newChildren, err := l.client.GetChildren(zkPath)
	if err != nil {
		log.Errorf("path{%s} child nodes changed, zk.Children() = error{%v}", zkPath, perrors.WithStack(err))
		return
	}
	var newNode string

	for _,n := range newChildren{
		if contains(children,n){
			continue
		}
		newNode = path.Join(zkPath, n)
		log.Infof("add zkNode{%s}", newNode)
		content, _, err := l.client.Conn.Get(newNode)
		if err != nil {
			log.Errorf("Get new node path {%v} 's content error,message is  {%v}", newNode, perrors.WithStack(err))
		}
		if !listener.DataChange(Event{Path:newNode,Action:EventTypeAdd,Content:string(content)}) {
			continue
		}
		go func(node string, zkPath string,listener DataListener) {
			log.Infof("delete zkNode{%s}", node)
			if l.ListenNodeEvent(zkPath,listener) {
				log.Infof("delete content{%s}", node)
				listener.DataChange(Event{Path: zkPath, Action: EventTypeDel})
			}
		}(newNode,zkPath,listener)
	}
	//老节点被删除
	var oldNode string
	for _,node := range children {
		if contains(children,node) {
			continue
		}
		oldNode = path.Join(zkPath, node)
		log.Warnf("delete zkPath{%s}", oldNode)
		listener.DataChange(Event{Path: oldNode, Action: EventTypeDel})
	}
}

func contains(src []string,dest string)bool  {
	for _,a := range src{
		if a == dest {
			return true
		}
	}
	return false
}

func timeSecondDuration(sec int) time.Duration {
	return time.Duration(sec) * time.Second
}
