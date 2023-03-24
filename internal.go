package queue

import (
	"time"

	. "github.com/infrago/base"
)

func (this *Module) Publish(name string, values ...Map) error {
	var value Map
	if len(values) > 0 {
		value = values[0]
	}
	return this.publish("", name, value)
}
func (this *Module) PublishTo(conn, name string, values ...Map) error {
	var value Map
	if len(values) > 0 {
		value = values[0]
	}
	return this.publish(conn, name, value)
}

func (this *Module) DeferredPublish(name string, value Map, delay time.Duration) error {
	return this.publish("", name, value, delay)
}
func (this *Module) DeferredPublishTo(conn, name string, value Map, delay time.Duration) error {
	return this.publish(conn, name, value, delay)
}
