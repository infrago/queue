package queue

import (
	"sync"
	"time"

	. "github.com/infrago/base"
	"github.com/infrago/infra"
	"github.com/infrago/util"
	"github.com/panjf2000/ants/v2"
)

func init() {
	infra.Mount(module)
}

var (
	module = &Module{
		configs:   make(map[string]Config, 0),
		drivers:   make(map[string]Driver, 0),
		instances: make(map[string]*Instance, 0),

		queues:   make(map[string]Queue, 0),
		filters:  make(map[string]Filter, 0),
		handlers: make(map[string]Handler, 0),
	}
)

type (
	Module struct {
		mutex sync.Mutex
		pool  *ants.Pool

		connected, initialized, launched bool

		configs map[string]Config
		drivers map[string]Driver

		queues   map[string]Queue
		filters  map[string]Filter
		handlers map[string]Handler

		relates map[string]string

		serveFilters    []ctxFunc
		requestFilters  []ctxFunc
		executeFilters  []ctxFunc
		responseFilters []ctxFunc

		foundHandlers  []ctxFunc
		errorHandlers  []ctxFunc
		failedHandlers []ctxFunc
		deniedHandlers []ctxFunc

		instances map[string]*Instance

		weights  map[string]int
		hashring *util.HashRing
	}

	Configs map[string]Config
	Config  struct {
		Driver  string
		Codec   string
		Weight  int
		Prefix  string
		Setting Map
	}
	Instance struct {
		module  *Module
		Name    string
		Config  Config
		connect Connect
	}
)

// Driver 注册驱动
func (module *Module) Driver(name string, driver Driver) {
	module.mutex.Lock()
	defer module.mutex.Unlock()

	if driver == nil {
		panic("Invalid queue driver: " + name)
	}

	if infra.Override() {
		module.drivers[name] = driver
	} else {
		if module.drivers[name] == nil {
			module.drivers[name] = driver
		}
	}
}

func (this *Module) Config(name string, config Config) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if name == "" {
		name = infra.DEFAULT
	}

	if infra.Override() {
		this.configs[name] = config
	} else {
		if _, ok := this.configs[name]; ok == false {
			this.configs[name] = config
		}
	}
}
func (this *Module) Configs(config Configs) {
	for key, val := range config {
		this.Config(key, val)
	}
}

// direct
func (this *Module) direct(conn, name string, meta *infra.Metadata) error {
	if name == "" {
		return errInvalidMsg
	}
	if conn == "" {
		conn = this.hashring.Locate(name)
	}

	inst, ok := module.instances[conn]
	if ok == false {
		return errInvalidConnection
	}

	bytes, err := infra.Marshal(inst.Config.Codec, &meta)
	if err != nil {
		return err
	}

	realName := inst.Config.Prefix + name
	return inst.connect.Enqueue(realName, bytes)

	return errInvalidConnection
}

// enqueue
func (this *Module) enqueue(to, name string, value Map, delays ...time.Duration) error {
	if name == "" {
		return errInvalidMsg
	}

	conn := infra.DEFAULT
	if to == "" {
		conn = this.hashring.Locate(name)
	} else {
		conn = to
	}

	inst, ok := module.instances[conn]
	if ok == false {
		return errInvalidConnection
	}

	if value == nil {
		value = Map{}
	}

	var dataBytes []byte

	//指定了to的，就走原始数据
	if to == "" {
		bytes, err := infra.Marshal(inst.Config.Codec, &value)
		if err != nil {
			return err
		}
		dataBytes = bytes
	} else {
		meta := infra.Metadata{Name: name, Payload: value}
		bytes, err := infra.Marshal(inst.Config.Codec, &meta)
		if err != nil {
			return err
		}
		dataBytes = bytes
	}

	realName := inst.Config.Prefix + name
	if len(delays) > 0 {
		return inst.connect.DeferredEnqueue(realName, dataBytes, delays[0])
	}
	return inst.connect.Enqueue(realName, dataBytes)
}
