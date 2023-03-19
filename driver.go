package queue

import "time"

type (
	// Driver 数据驱动
	Driver interface {
		Connect(*Instance) (Connect, error)
	}
	Health struct {
		Workload int64
	}

	Delegate interface {
		Serve(string, []byte)
	}

	// Connect 连接
	Connect interface {
		Open() error
		Health() (Health, error)
		Close() error

		Register(Info) error

		Start() error
		Stop() error

		Enqueue(name string, data []byte) error
		DeferredEnqueue(name string, data []byte, delay time.Duration) error
	}
)
