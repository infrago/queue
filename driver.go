package queue

import (
	"time"

	. "github.com/infrago/base"
)

type (
	// Driver 数据驱动
	Driver interface {
		Connect(*Instance) (Connect, error)
	}
	Health struct {
		Workload int64
	}

	// Connect 连接
	Connect interface {
		Open() error
		Health() (Health, error)
		Close() error

		Register(string) error

		Start() error
		Stop() error

		Publish(name string, data []byte) error
		DeferredPublish(name string, data []byte, delay time.Duration) error
	}
	Instance struct {
		connect Connect
		Name    string
		Config  Config
		Setting Map
	}
)
