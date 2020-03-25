package zookeeper

import "fmt"

type DataListener interface {
	DataChange(eventType Event) bool //bool is return for interface implement is interesting
}

//////////////////////////////////////////
// event type
//////////////////////////////////////////

type EventType int

const (
	EventTypeAdd = iota
	EventTypeDel
	EventTypeUpdate
)

var serviceEventTypeStrings = [...]string{
	"add",
	"delete",
	"update",
}

func (t EventType) String() string {
	return serviceEventTypeStrings[t]
}


type Event struct {
	Path    string
	Action  EventType
	Content string
}

func (e Event) String() string {
	return fmt.Sprintf("Event{Action{%s}, Content{%s}}", e.Action, e.Content)
}
