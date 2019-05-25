package sinks

import (
	"bytes"
	"context"

	eventhub "github.com/Azure/azure-event-hubs-go"
	"github.com/golang/glog"
	v1 "k8s.io/api/core/v1"
)

type EventHubSink struct {
	hub *eventhub.Hub
}

func NewEventHubSink(connString string) (*EventHubSink, error) {
	hub, err := eventhub.NewHubFromConnectionString(connString)
	if err != nil {
		return nil, err
	}
	return &EventHubSink{hub: hub}, nil
}

// UpdateEvents implements the EventSinkInterface
func (ehs *EventHubSink) UpdateEvents(eNew *v1.Event, eOld *v1.Event) {
	eData := NewEventData(eNew, eOld)
	var buf bytes.Buffer
	if _, err := eData.WriteFlattenedJSON(&buf); err == nil {
		glog.Info(buf.String())
		ehs.hub.Send(context.TODO(), eventhub.NewEventFromString(buf.String()))
	} else {
		glog.Warningf("Failed to flatten json: %v", err)
	}
}
