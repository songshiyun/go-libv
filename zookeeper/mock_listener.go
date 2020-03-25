package zookeeper

import (
	log "github.com/sirupsen/logrus"
	_ "github.com/songshiyun/go-libv/log"
)

type MockListener struct {}


func (m *MockListener)DataChange(eventType Event) bool  {
	log.WithFields(log.Fields{
		"path":eventType.Path,
		"action":eventType.Action,
		"content":eventType.Content,
	}).Info("mock listener")
	return true
}