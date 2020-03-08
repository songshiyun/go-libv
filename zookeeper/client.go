package zookeeper

import (
	"path"
	"strings"
	"sync"
	"time"

	perrors "github.com/pkg/errors"
	"github.com/samuel/go-zookeeper/zk"
	log "github.com/sirupsen/logrus"

	_"github.com/songshiyun/go-libv/log"
)

var errNilZkClientConn = perrors.New("zookeeperclient{conn} is nil")

type ZookeeperClient struct {
	name          string
	ZkAddrs       []string
	sync.Mutex    // for conn
	Conn          *zk.Conn
	Timeout       time.Duration
	exit          chan struct{}
	Wait          sync.WaitGroup
	eventRegistry map[string][]*chan struct{}
}

func StateToString(state zk.State) string {
	switch state {
	case zk.StateDisconnected:
		return "zookeeper disconnected"
	case zk.StateConnecting:
		return "zookeeper connecting"
	case zk.StateAuthFailed:
		return "zookeeper auth failed"
	case zk.StateConnectedReadOnly:
		return "zookeeper connect readonly"
	case zk.StateSaslAuthenticated:
		return "zookeeper sasl authenticated"
	case zk.StateExpired:
		return "zookeeper connection expired"
	case zk.StateConnected:
		return "zookeeper connected"
	case zk.StateHasSession:
		return "zookeeper has session"
	case zk.StateUnknown:
		return "zookeeper unknown state"
	case zk.State(zk.EventNodeDeleted):
		return "zookeeper node deleted"
	case zk.State(zk.EventNodeDataChanged):
		return "zookeeper node data changed"
	default:
		return state.String()
	}
}

func NewClient(name string,addrs []string,timeout time.Duration) (*ZookeeperClient, error) {
	return newZookeeperClient(name,addrs,timeout)
}

func newZookeeperClient(name string, zkAddrs []string, timeout time.Duration) (*ZookeeperClient, error) {
	var (
		err   error
		event <-chan zk.Event
		z     *ZookeeperClient
	)
	z = &ZookeeperClient{
		name:          name,
		ZkAddrs:       zkAddrs,
		Timeout:       timeout,
		exit:          make(chan struct{}),
		eventRegistry: make(map[string][]*chan struct{}),
	}
	// connect to zookeeper
	z.Conn, event, err = zk.Connect(zkAddrs, timeout)
	if err != nil {
		return nil, perrors.WithMessagef(err, "zk.Connect(zkAddrs:%+v)", zkAddrs)
	}
	z.Wait.Add(1)
	go z.HandleZkEvent(event)
	return z, nil
}

func (z *ZookeeperClient) HandleZkEvent(session <-chan zk.Event) {
	var (
		state int
		event zk.Event
	)
	defer func() {
		z.Wait.Done()
	}()
LOOP:
	for {
		select {
		case <-z.exit:
			break LOOP
		case event = <-session:
			log.WithFields(log.Fields{
				"client":z.name,"type":event.Type,"server":event.Server,"path":event.Path,"state":StateToString(event.State),"error":event.Err,
			}).Info("get a zookeeper event")
			switch (int)(event.State) {
			case (int)(zk.StateDisconnected):
				log.Warnf("zk{addr:%s} state is StateDisconnected, so close the zk client{name:%s}.", z.ZkAddrs, z.name)
				z.stop()
				z.Lock()
				if z.Conn != nil {
					z.Conn.Close()
					z.Conn = nil
				}
				z.Unlock()
				break LOOP
			case (int)(zk.EventNodeDataChanged), (int)(zk.EventNodeChildrenChanged):
				log.Infof("zkClient{%s} get zk node changed event{path:%s}", z.name, event.Path)
				z.Lock()
				for p, a := range z.eventRegistry {
					if strings.HasPrefix(p, event.Path) {
						log.Infof("send event{state:zk.EventNodeDataChange, Path:%s} notify event to path{%s} related listener",
							event.Path, p)
						for _, e := range a {
							*e <- struct{}{}
						}
					}
				}
				z.Unlock()
			case (int)(zk.StateConnecting), (int)(zk.StateConnected), (int)(zk.StateHasSession):
				if state == (int)(zk.StateHasSession) {
					continue
				}
				if a, ok := z.eventRegistry[event.Path]; ok && 0 < len(a) {
					for _, e := range a {
						*e <- struct{}{}
					}
				}
			}
			state = (int)(event.State)
		}
	}
}

func (z *ZookeeperClient) RegisterEvent(zkPath string, event *chan struct{}) {
	if zkPath == "" || event == nil {
		return
	}
	z.Lock()
	a := z.eventRegistry[zkPath]
	a = append(a, event)
	z.eventRegistry[zkPath] = a
	log.Debugf("zkClient{%s} register event{path:%s, ptr:%p}", z.name, zkPath, event)
	z.Unlock()
}

func (z *ZookeeperClient) UnregisterEvent(zkPath string, event *chan struct{}) {
	if zkPath == "" {
		return
	}
	z.Lock()
	defer z.Unlock()
	infoList, ok := z.eventRegistry[zkPath]
	if !ok {
		return
	}
	for i, e := range infoList {
		if e == event {
			arr := infoList
			infoList = append(arr[:i], arr[i+1:]...)
			log.Infof("zkClient{%s} unregister event{path:%s, event:%p}", z.name, zkPath, event)
		}
	}
	log.Debugf("after zkClient{%s} unregister event{path:%s, event:%p}, array length %d",
		z.name, zkPath, event, len(infoList))
	if len(infoList) == 0 {
		delete(z.eventRegistry, zkPath)
	} else {
		z.eventRegistry[zkPath] = infoList
	}
}

func (z *ZookeeperClient) Done() <-chan struct{} {
	return z.exit
}
func (z *ZookeeperClient) ZkConnValid() bool {
	select {
	case <-z.exit:
		return false
	default:
	}

	valid := true
	z.Lock()
	if z.Conn == nil {
		valid = false
	}
	z.Unlock()

	return valid
}

func (z *ZookeeperClient) Close() {
	if z == nil {
		return
	}

	z.stop()
	z.Wait.Wait()
	z.Lock()
	if z.Conn != nil {
		z.Conn.Close()
		z.Conn = nil
	}
	z.Unlock()
	log.Warnf("zkClient{name:%s, zk addr:%s} exit now.", z.name, z.ZkAddrs)
}

func (z *ZookeeperClient) Create(basePath string) error {
	var (
		err     error
		tmpPath string
	)
	log.Debugf("zookeeperClient.Create(basePath{%s})", basePath)
	for _, str := range strings.Split(basePath, "/")[1:] {
		tmpPath = path.Join(tmpPath, "/", str)
		err = errNilZkClientConn
		z.Lock()
		if z.Conn != nil {
			_, err = z.Conn.Create(tmpPath, []byte(""), 0, zk.WorldACL(zk.PermAll))
		}
		z.Unlock()
		if err != nil {
			if err == zk.ErrNodeExists {
				log.Infof("zk.create(\"%s\") exists", tmpPath)
			} else {
				log.Errorf("zk.create(\"%s\") error(%v)", tmpPath, perrors.WithStack(err))
				return perrors.WithMessagef(err, "zk.Create(path:%s)", basePath)
			}
		}
	}

	return nil
}

func (z *ZookeeperClient) Delete(basePath string) error {
	var (
		err error
	)

	err = errNilZkClientConn
	z.Lock()
	if z.Conn != nil {
		err = z.Conn.Delete(basePath, -1)
	}
	z.Unlock()

	return perrors.WithMessagef(err, "Delete(basePath:%s)", basePath)
}

func (z *ZookeeperClient) RegisterTemp(basePath string, node string) (string, error) {
	var (
		err     error
		data    []byte
		zkPath  string
		tmpPath string
	)
	err = errNilZkClientConn
	data = []byte("")
	zkPath = path.Join(basePath) + "/" + node
	z.Lock()
	if z.Conn != nil {
		tmpPath, err = z.Conn.Create(zkPath, data, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	}
	z.Unlock()
	//if err != nil && err != zk.ErrNodeExists {
	if err != nil {
		log.Warnf("conn.Create(\"%s\", zk.FlagEphemeral) = error(%v)", zkPath, perrors.WithStack(err))
		return "", perrors.WithStack(err)
	}
	log.Debugf("zkClient{%s} create a temp zookeeper node:%s", z.name, tmpPath)

	return tmpPath, nil
}

func (z *ZookeeperClient) RegisterTempSeq(basePath string, data []byte) (string, error) {
	var (
		err     error
		tmpPath string
	)
	err = errNilZkClientConn
	z.Lock()
	if z.Conn != nil {
		tmpPath, err = z.Conn.Create(
			path.Join(basePath)+"/",
			data,
			zk.FlagEphemeral|zk.FlagSequence,
			zk.WorldACL(zk.PermAll),
		)
	}
	z.Unlock()
	log.Debugf("zookeeperClient.RegisterTempSeq(basePath{%s}) = tempPath{%s}", basePath, tmpPath)
	if err != nil && err != zk.ErrNodeExists {
		log.Errorf("zkClient{%s} conn.Create(\"%s\", \"%s\", zk.FlagEphemeral|zk.FlagSequence) error(%v)",
			z.name, basePath, string(data), err)
		return "", perrors.WithStack(err)
	}
	log.Debugf("zkClient{%s} create a temp zookeeper node:%s", z.name, tmpPath)
	return tmpPath, nil
}

func (z *ZookeeperClient) GetChildrenW(path string) ([]string, <-chan zk.Event, error) {
	var (
		err      error
		children []string
		stat     *zk.Stat
		event    <-chan zk.Event
	)

	err = errNilZkClientConn
	z.Lock()
	if z.Conn != nil {
		children, stat, event, err = z.Conn.ChildrenW(path)
	}
	z.Unlock()
	if err != nil {
		if err == zk.ErrNoNode {
			return nil, nil, perrors.Errorf("path{%s} has none children", path)
		}
		log.Errorf("zk.ChildrenW(path{%s}) = error(%v)", path, err)
		return nil, nil, perrors.WithMessagef(err, "zk.ChildrenW(path:%s)", path)
	}
	if stat == nil {
		return nil, nil, perrors.Errorf("path{%s} has none children", path)
	}
	if len(children) == 0 {
		return nil, nil, perrors.Errorf("path{%s} has none children", path)
	}

	return children, event, nil
}

func (z *ZookeeperClient) GetChildren(path string) ([]string, error) {
	var (
		err      error
		children []string
		stat     *zk.Stat
	)

	err = errNilZkClientConn
	z.Lock()
	if z.Conn != nil {
		children, stat, err = z.Conn.Children(path)
	}
	z.Unlock()
	if err != nil {
		if err == zk.ErrNoNode {
			return nil, perrors.Errorf("path{%s} has none children", path)
		}
		log.Errorf("zk.Children(path{%s}) = error(%v)", path, perrors.WithStack(err))
		return nil, perrors.WithMessagef(err, "zk.Children(path:%s)", path)
	}
	if stat == nil {
		return nil, perrors.Errorf("path{%s} has none children", path)
	}
	if len(children) == 0 {
		return nil, perrors.Errorf("path{%s} has none children", path)
	}

	return children, nil
}

func (z *ZookeeperClient) ExistW(zkPath string) (<-chan zk.Event, error) {
	var (
		exist bool
		err   error
		event <-chan zk.Event
	)

	err = errNilZkClientConn
	z.Lock()
	if z.Conn != nil {
		exist, _, event, err = z.Conn.ExistsW(zkPath)
	}
	z.Unlock()
	if err != nil {
		log.Warnf("zkClient{%s}.ExistsW(path{%s}) = error{%v}.", z.name, zkPath, perrors.WithStack(err))
		return nil, perrors.WithMessagef(err, "zk.ExistsW(path:%s)", zkPath)
	}
	if !exist {
		log.Warnf("zkClient{%s}'s App zk path{%s} does not exist.", z.name, zkPath)
		return nil, perrors.Errorf("zkClient{%s} App zk path{%s} does not exist.", z.name, zkPath)
	}

	return event, nil
}

func (z *ZookeeperClient) GetContent(zkPath string) ([]byte, *zk.Stat, error) {
	return z.Conn.Get(zkPath)
}

func (z *ZookeeperClient) stop() bool {
	select {
	case <-z.exit:
		return true
	default:
		close(z.exit)
	}

	return false
}