package queue

import (
	"time"

	. "github.com/infrago/base"
)

func (this *Module) Enqueue(name string, values ...Map) error {
	var value Map
	if len(values) > 0 {
		value = values[0]
	}
	return this.enqueue("", name, value)
}
func (this *Module) DeferredEnqueue(name string, value Map, delay time.Duration) error {
	return this.enqueue("", name, value, delay)
}

func (this *Module) EnqueueTo(conn, name string, values ...Map) error {
	var value Map
	if len(values) > 0 {
		value = values[0]
	}
	return this.enqueue(conn, name, value)
}
func (this *Module) DeferredEnqueueTo(conn, name string, value Map, delay time.Duration) error {
	return this.enqueue(conn, name, value, delay)
}
