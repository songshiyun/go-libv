package zookeeper

import (
	"testing"
	"time"
)

func TestNewClient(t *testing.T) {
	client,err := NewClient("test-zk",[]string{"10.12.33.33"},10*time.Second)
	if err != nil {
		t.Errorf("new zookeeper client err: %v",err)
	}else {
		t.Logf("%v,%v,%v,%v",client.name,client.Conn,client.Conn,client.exit)
	}
}

func TestGetContent(t *testing.T)  {
	client,err := NewClient("test-zk",[]string{"10.12.33.33"},10*time.Second)
	if err != nil {
		t.Errorf("new zookeeper client err: %v",err)
	}else {
		byts, _, err := client.GetContent("/latest_producer_id_block")
		if err != nil {
			t.Errorf("get path of / content err: %v",err)
		}else {
			t.Logf("contents: %s",string(byts))
		}
	}
}

func TestZookeeperClient_GetChildren(t *testing.T) {
	client,err := NewClient("test-zk",[]string{"10.12.33.33"},10*time.Second)
	if err != nil {
		t.Errorf("new zookeeper client err: %v",err)
	}else {
		strs,err := client.GetChildren("/")
		if err != nil {
			t.Errorf("get children err: %v",err)
		}else {
			t.Logf("children: %v",strs)
		}
	}
}

func TestZookeeperClient_Create(t *testing.T) {
	client,err := NewClient("test-zk",[]string{"10.12.33.33"},10*time.Second)
	if err != nil {
		t.Errorf("new zookeeper client err: %v",err)
	}else{
		err := client.Create("/zookeeper-go-test/go-config-list")
		if err != nil {
			t.Errorf("create zk node err: %v",err)
		}else {
			t.Logf("create zk node success")
		}
	}
}

func TestZookeeperClient_Set(t *testing.T) {
	client,err := NewClient("test-zk",[]string{"10.12.33.33"},10*time.Second)
	if err != nil {
		t.Errorf("new zookeeper client err: %v",err)
	}else{
		stat,err := client.Set("/zookeeper-go-test/go-config-list",[]byte(`{"name":"songshiyun","age":"25"}`),1000000000)
		if err != nil {
			t.Errorf("set zk node err: %v",err)
		}else {
			t.Logf("version:%v",stat.Version)
			t.Logf("set zk node success")
		}
	}
}