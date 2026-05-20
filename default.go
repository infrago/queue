package queue

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	. "github.com/infrago/base"
	"github.com/infrago/infra"
)

func init() {
	infra.Register(infra.DEFAULT, &defaultDriver{})
}

var (
	errQueueRunning    = errors.New("queue is running")
	errQueueNotRunning = errors.New("queue is not running")
	errQueueFull       = errors.New("queue is full")
)

type (
	defaultDriver struct{}

	defaultConnection struct {
		mutex    sync.RWMutex
		running  bool
		stopping bool
		stopped  bool
		instance *Instance
		queues   map[string]chan *defaultMessage
		workers  map[string]int
		timers   map[*time.Timer]struct{}
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
		workers:  make(map[string]int, 0),
		timers:   make(map[*time.Timer]struct{}, 0),
		done:     make(chan struct{}),
	}, nil
}

func (c *defaultConnection) Open() error  { return nil }
func (c *defaultConnection) Close() error { return nil }

func (c *defaultConnection) Register(name string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if _, ok := c.queues[name]; !ok {
		c.queues[name] = make(chan *defaultMessage, c.buffer(name))
	}
	c.workers[name]++
	return nil
}

func (c *defaultConnection) Start() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.running || c.stopping {
		return errQueueRunning
	}
	if c.stopped {
		c.done = make(chan struct{})
		c.stopped = false
	}
	for name, workers := range c.workers {
		queueCh := c.queues[name]
		for i := 0; i < workers; i++ {
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
							if err := c.publish(msg, res.Delay, true); err != nil {
								logDefaultError("retry", msg.name, err)
							}
						}
					case <-c.done:
						return
					}
				}
			}()
		}
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
	for timer := range c.timers {
		timer.Stop()
		delete(c.timers, timer)
	}
	c.mutex.Unlock()

	c.wg.Wait()

	c.mutex.Lock()
	c.running = false
	c.stopping = false
	c.stopped = true
	return nil
}

func (c *defaultConnection) publish(msg *defaultMessage, delay time.Duration, wait bool) error {
	c.mutex.RLock()
	ch := c.queues[msg.name]
	done := c.done
	stopped := c.stopped
	c.mutex.RUnlock()
	if stopped {
		return errQueueNotRunning
	}
	if ch == nil {
		return errInvalidQueue
	}
	if delay > 0 {
		var timer *time.Timer
		timer = time.AfterFunc(delay, func() {
			c.removeTimer(timer)
			_ = sendDefaultMessage(ch, msg, done, true)
		})
		c.addTimer(timer)
		return nil
	}
	return sendDefaultMessage(ch, msg, done, wait)
}

func sendDefaultMessage(ch chan *defaultMessage, msg *defaultMessage, done <-chan struct{}, wait bool) error {
	if wait {
		select {
		case ch <- msg:
			return nil
		case <-done:
			return errQueueNotRunning
		}
	}

	select {
	case ch <- msg:
		return nil
	case <-done:
		return errQueueNotRunning
	default:
		return errQueueFull
	}
}

func (c *defaultConnection) Publish(name string, data []byte) error {
	wait, timeout := c.publishWait()
	if wait && timeout > 0 {
		return c.publishTimeout(&defaultMessage{name: name, data: data, attempt: 1}, timeout)
	}
	return c.publish(&defaultMessage{name: name, data: data, attempt: 1}, 0, wait)
}

func (c *defaultConnection) DeferredPublish(name string, data []byte, delay time.Duration) error {
	return c.publish(&defaultMessage{name: name, data: data, attempt: 1}, delay, false)
}

func (c *defaultConnection) publishTimeout(msg *defaultMessage, timeout time.Duration) error {
	c.mutex.RLock()
	ch := c.queues[msg.name]
	done := c.done
	stopped := c.stopped
	c.mutex.RUnlock()
	if stopped {
		return errQueueNotRunning
	}
	if ch == nil {
		return errInvalidQueue
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case ch <- msg:
		return nil
	case <-done:
		return errQueueNotRunning
	case <-timer.C:
		return errQueueFull
	}
}

func (c *defaultConnection) publishWait() (bool, time.Duration) {
	if c.instance == nil {
		return false, 0
	}
	wait := boolSetting(c.instance.Setting, "blocking_publish", false)
	timeout := durationSetting(c.instance.Setting, "publish_timeout", 0)
	if timeout > 0 {
		wait = true
	}
	return wait, timeout
}

func (c *defaultConnection) buffer(name string) int {
	if c.instance != nil {
		queueName := name
		prefix := c.instance.Config.Prefix
		if prefix != "" && strings.HasPrefix(queueName, prefix) {
			queueName = strings.TrimPrefix(queueName, prefix)
		}
		if cfg, ok := module.queues[queueName]; ok {
			if size := intSetting(cfg.Setting, "buffer", 0); size > 0 {
				return size
			}
		}
		if size := intSetting(c.instance.Setting, "buffer", 0); size > 0 {
			return size
		}
	}
	return 256
}

func (c *defaultConnection) addTimer(timer *time.Timer) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	select {
	case <-c.done:
		timer.Stop()
	default:
		c.timers[timer] = struct{}{}
	}
}

func (c *defaultConnection) removeTimer(timer *time.Timer) {
	c.mutex.Lock()
	delete(c.timers, timer)
	c.mutex.Unlock()
}

func intSetting(setting Map, key string, fallback int) int {
	if setting == nil {
		return fallback
	}
	switch v := setting[key].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	case string:
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func boolSetting(setting Map, key string, fallback bool) bool {
	if setting == nil {
		return fallback
	}
	switch v := setting[key].(type) {
	case bool:
		return v
	case string:
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "true", "1", "yes", "on":
			return true
		case "false", "0", "no", "off":
			return false
		}
	}
	return fallback
}

func durationSetting(setting Map, key string, fallback time.Duration) time.Duration {
	if setting == nil {
		return fallback
	}
	switch v := setting[key].(type) {
	case time.Duration:
		return v
	case int:
		return time.Duration(v) * time.Second
	case int64:
		return time.Duration(v) * time.Second
	case float64:
		return time.Duration(v * float64(time.Second))
	case string:
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		if n, err := strconv.Atoi(v); err == nil {
			return time.Duration(n) * time.Second
		}
	}
	return fallback
}

func logDefaultError(action, name string, err error) {
	if err != nil {
		fmt.Printf("infrago queue default %s failed on %s: %v\n", action, name, err)
	}
}
