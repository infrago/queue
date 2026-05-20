package queue

import (
	"context"
	"sync"
	"time"

	base "github.com/infrago/base"
)

type (
	Driver interface {
		Connect(*Instance) (Connection, error)
	}

	Connection interface {
		Open() error
		Close() error
		Start() error
		Stop() error

		Register(name string) error
		Publish(name string, data []byte) error
		DeferredPublish(name string, data []byte, delay time.Duration) error
	}

	Instance struct {
		conn    Connection
		mutex   sync.RWMutex
		ctx     context.Context
		cancel  context.CancelFunc
		Name    string
		Config  Config
		Setting base.Map
	}
)

func (inst *Instance) context() context.Context {
	inst.mutex.RLock()
	defer inst.mutex.RUnlock()
	if inst.ctx == nil {
		return context.Background()
	}
	return inst.ctx
}

func (inst *Instance) resetContext() {
	inst.mutex.Lock()
	defer inst.mutex.Unlock()
	if inst.cancel != nil {
		inst.cancel()
	}
	inst.ctx, inst.cancel = context.WithCancel(context.Background())
}

func (inst *Instance) cancelContext() {
	inst.mutex.Lock()
	defer inst.mutex.Unlock()
	if inst.cancel != nil {
		inst.cancel()
	}
}
