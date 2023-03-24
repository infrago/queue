package queue

import (
	"time"

	. "github.com/infrago/base"
)

func Publish(name string, values ...Map) error {
	return module.Publish(name, values...)
}
func DeferredPublish(name string, value Map, delay time.Duration) error {
	return module.DeferredPublish(name, value, delay)
}

func PublishTo(conn, name string, values ...Map) error {
	return module.PublishTo(conn, name, values...)
}
func DeferredPublishTo(conn, name string, value Map, delay time.Duration) error {
	return module.DeferredPublishTo(conn, name, value, delay)
}
