package queue

import (
	"time"

	. "github.com/infrago/base"
)

func Enqueue(name string, values ...Map) error {
	return module.Enqueue(name, values...)
}
func DeferredEnqueue(name string, value Map, delay time.Duration) error {
	return module.DeferredEnqueue(name, value, delay)
}

func EnqueueTo(conn, name string, values ...Map) error {
	return module.EnqueueTo(conn, name, values...)
}
func DeferredEnqueueTo(conn, name string, value Map, delay time.Duration) error {
	return module.DeferredEnqueueTo(conn, name, value, delay)
}
