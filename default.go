package queue

import (
	"errors"
	"sync"
	"time"

	"github.com/infrago/infra"
)

func init() {
	infra.Register(infra.DEFAULT, &defaultDriver{})
}

var (
	errQueueRunning    = errors.New("queue is running")
	errQueueNotRunning = errors.New("queue is not running")
)

type (
	defaultDriver struct{}

	defaultConnection struct {
		mutex    sync.RWMutex
		running  bool
		stopping bool
		instance *Instance
		queues   map[string]chan *defaultMessage
		names    []string
		done     chan struct{}
		wg       sync.WaitGroup
	}

	defaultMessage struct {
		name    string
		data    []byte
		attempt int
	}
)

func (d *defaultDriver) Connect(inst *Instance) (Connection, error) {
	return &defaultConnection{
		instance: inst,
		queues:   make(map[string]chan *defaultMessage, 0),
		names:    make([]string, 0),
		done:     make(chan struct{}),
	}, nil
}

func (c *defaultConnection) Open() error  { return nil }
func (c *defaultConnection) Close() error { return nil }

func (c *defaultConnection) Register(name string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if _, ok := c.queues[name]; !ok {
		c.queues[name] = make(chan *defaultMessage, 256)
	}
	c.names = append(c.names, name)
	return nil
}

func (c *defaultConnection) Start() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.running || c.stopping {
		return errQueueRunning
	}
	for _, name := range c.names {
		queueCh := c.queues[name]
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			for {
				select {
				case msg := <-queueCh:
					if msg == nil {
						continue
					}
					req := Request{
						Name:      msg.name,
						Data:      msg.data,
						Attempt:   msg.attempt,
						Timestamp: time.Now(),
					}
					res := c.instance.Serve(req)
					if res.Retry {
						msg.attempt++
						c.publish(msg, res.Delay)
					}
				case <-c.done:
					return
				}
			}
		}()
	}
	c.running = true
	return nil
}

func (c *defaultConnection) Stop() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if !c.running || c.stopping {
		return errQueueNotRunning
	}
	close(c.done)
	c.stopping = true
	c.mutex.Unlock()

	c.wg.Wait()

	c.mutex.Lock()
	c.done = make(chan struct{})
	c.running = false
	c.stopping = false
	return nil
}

func (c *defaultConnection) publish(msg *defaultMessage, delay time.Duration) {
	c.mutex.RLock()
	ch := c.queues[msg.name]
	done := c.done
	c.mutex.RUnlock()
	if ch == nil {
		return
	}
	if delay > 0 {
		time.AfterFunc(delay, func() {
			select {
			case ch <- msg:
			case <-done:
			}
		})
		return
	}
	select {
	case ch <- msg:
	case <-done:
	}
}

func (c *defaultConnection) Publish(name string, data []byte) error {
	c.publish(&defaultMessage{name: name, data: data, attempt: 1}, 0)
	return nil
}

func (c *defaultConnection) DeferredPublish(name string, data []byte, delay time.Duration) error {
	c.publish(&defaultMessage{name: name, data: data, attempt: 1}, delay)
	return nil
}
