package zookeeper

import (
	"sync"
	"testing"
	"time"
)

func TestZkEventListener_ListenServiceEvent(t *testing.T) {
	client,err := NewClient("test-zk",[]string{"10.12.33.33"},10*time.Second)
	if err != nil {
		t.Errorf("new zookeeper client err: %v",err)
	}else {
		content := `
		  system_name = "xt"
          system_token = "120101010slalala"
          protocol = "http"
          host = "uuap.rg010.com"`

		var wait sync.WaitGroup
		wait.Add(1)
		listener := NewZKListenner(client)
		dataListener := &MockListener{}
		listener.ListenServiceEvent("/zookeeper-go-test",dataListener)
		_, err := client.Conn.Set("/zookeeper-go-test/go-config", []byte(content),1)
		if err != nil {
			t.Errorf("set content err: %v",err)
		}
		wait.Wait()
	}

}