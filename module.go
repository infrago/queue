package queue

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	. "github.com/infrago/base"
	"github.com/infrago/infra"
	"github.com/infrago/util"
)

var (
	errInvalidQueue = errors.New("invalid queue name")
	errNoConnection = errors.New("invalid queue connection")
)

func init() {
	infra.Mount(module)
}

var module = &Module{
	configs:   make(map[string]Config, 0),
	drivers:   make(map[string]Driver, 0),
	instances: make(map[string]*Instance, 0),

	queues:   make(map[string]Queue, 0),
	declares: make(map[string]Declare, 0),
	filters:  make(map[string]Filter, 0),
	handlers: make(map[string]Handler, 0),
}

type (
	Module struct {
		mutex sync.RWMutex

		opened  bool
		started bool

		configs map[string]Config
		drivers map[string]Driver

		queues   map[string]Queue
		declares map[string]Declare
		filters  map[string]Filter
		handlers map[string]Handler

		serveFilters    []ctxFunc
		requestFilters  []ctxFunc
		executeFilters  []ctxFunc
		responseFilters []ctxFunc
		foundHandlers   []ctxFunc
		errorHandlers   []ctxFunc
		failedHandlers  []ctxFunc
		deniedHandlers  []ctxFunc

		instances map[string]*Instance
		weights   map[string]int
		hashring  *util.HashRing
	}

	Configs map[string]Config

	Config struct {
		Driver   string
		External bool
		Codec    string
		Weight   int
		Prefix   string
		Setting  Map
	}

	msgEnvelope struct {
		Name     string         `json:"name"`
		Metadata infra.Metadata `json:"metadata"`
		Payload  Map            `json:"payload"`
	}
)

func (m *Module) Register(name string, value Any) {
	switch v := value.(type) {
	case Driver:
		m.RegisterDriver(name, v)
	case Config:
		m.RegisterConfig(name, v)
	case Configs:
		m.RegisterConfigs(v)
	case Queue:
		m.RegisterQueue(name, v)
	case Queues:
		m.RegisterQueues(name, v)
	case Declare:
		m.RegisterDeclare(name, v)
	case Filter:
		m.RegisterFilter(name, v)
	case Handler:
		m.RegisterHandler(name, v)
	}
}

func (m *Module) RegisterQueues(prefix string, queues Queues) {
	for name, queue := range queues {
		target := name
		if prefix != "" {
			target = prefix + "." + name
		}
		m.RegisterQueue(target, queue)
	}
}

func (m *Module) RegisterDriver(name string, driver Driver) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if name == "" {
		name = infra.DEFAULT
	}
	if driver == nil {
		panic("invalid queue driver: " + name)
	}
	if _, ok := m.drivers[name]; ok {
		panic("queue driver already registered: " + name)
	}
	m.drivers[name] = driver
}

func (m *Module) RegisterConfig(name string, cfg Config) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.opened || m.started {
		return
	}
	if name == "" {
		name = infra.DEFAULT
	}
	if _, ok := m.configs[name]; ok {
		panic("queue config already registered: " + name)
	}
	m.configs[name] = cfg
}

func (m *Module) RegisterConfigs(configs Configs) {
	for name, cfg := range configs {
		m.RegisterConfig(name, cfg)
	}
}

func (m *Module) Config(global Map) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.opened || m.started {
		return
	}

	cfgAny, ok := global["queue"]
	if !ok {
		return
	}
	cfgMap, ok := cfgAny.(Map)
	if !ok || cfgMap == nil {
		return
	}

	root := Map{}
	for key, val := range cfgMap {
		if conf, ok := val.(Map); ok && key != "setting" {
			m.configure(key, conf)
		} else {
			root[key] = val
		}
	}
	if len(root) > 0 {
		m.configure(infra.DEFAULT, root)
	}
}

func (m *Module) configure(name string, conf Map) {
	cfg := Config{
		Driver: infra.DEFAULT,
		Codec:  infra.GOB,
		Weight: 1,
	}
	if existed, ok := m.configs[name]; ok {
		cfg = existed
	}

	if v, ok := conf["driver"].(string); ok && v != "" {
		cfg.Driver = v
	}
	if v, ok := conf["external"].(bool); ok {
		cfg.External = v
	}
	if v, ok := conf["codec"].(string); ok && v != "" {
		cfg.Codec = v
	}
	if v, ok := conf["prefix"].(string); ok {
		cfg.Prefix = v
	}
	if v, ok := conf["weight"].(int); ok {
		cfg.Weight = v
	}
	if v, ok := conf["weight"].(int64); ok {
		cfg.Weight = int(v)
	}
	if v, ok := conf["weight"].(float64); ok {
		cfg.Weight = int(v)
	}
	if v, ok := conf["weight"].(string); ok {
		if w, err := strconv.Atoi(v); err == nil {
			cfg.Weight = w
		}
	}
	if v, ok := conf["setting"].(Map); ok {
		cfg.Setting = v
	}

	m.configs[name] = cfg
}

func (m *Module) Setup() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if len(m.configs) == 0 {
		m.configs[infra.DEFAULT] = Config{
			Driver: infra.DEFAULT,
			Codec:  infra.GOB,
			Weight: 1,
		}
	}

	for name, cfg := range m.configs {
		if cfg.Driver == "" {
			cfg.Driver = infra.DEFAULT
		}
		if cfg.Codec == "" {
			cfg.Codec = infra.GOB
		}
		if cfg.Weight == 0 {
			cfg.Weight = 1
		}
		m.configs[name] = cfg
	}

	for name, q := range m.queues {
		if q.Thread <= 0 {
			q.Thread = 1
		}
		m.queues[name] = q
	}

	m.serveFilters = make([]ctxFunc, 0)
	m.requestFilters = make([]ctxFunc, 0)
	m.executeFilters = make([]ctxFunc, 0)
	m.responseFilters = make([]ctxFunc, 0)
	for _, f := range m.filters {
		if f.Serve != nil {
			m.serveFilters = append(m.serveFilters, f.Serve)
		}
		if f.Request != nil {
			m.requestFilters = append(m.requestFilters, f.Request)
		}
		if f.Execute != nil {
			m.executeFilters = append(m.executeFilters, f.Execute)
		}
		if f.Response != nil {
			m.responseFilters = append(m.responseFilters, f.Response)
		}
	}

	m.foundHandlers = make([]ctxFunc, 0)
	m.errorHandlers = make([]ctxFunc, 0)
	m.failedHandlers = make([]ctxFunc, 0)
	m.deniedHandlers = make([]ctxFunc, 0)
	for _, h := range m.handlers {
		if h.Found != nil {
			m.foundHandlers = append(m.foundHandlers, h.Found)
		}
		if h.Error != nil {
			m.errorHandlers = append(m.errorHandlers, h.Error)
		}
		if h.Failed != nil {
			m.failedHandlers = append(m.failedHandlers, h.Failed)
		}
		if h.Denied != nil {
			m.deniedHandlers = append(m.deniedHandlers, h.Denied)
		}
	}
}

func (m *Module) Open() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.opened {
		return
	}

	weights := make(map[string]int, 0)
	for name, cfg := range m.configs {
		driver, ok := m.drivers[cfg.Driver]
		if !ok || driver == nil {
			panic("missing queue driver: " + cfg.Driver)
		}

		inst := &Instance{
			Name:    name,
			Config:  cfg,
			Setting: cfg.Setting,
		}

		conn, err := driver.Connect(inst)
		if err != nil {
			panic("failed to connect queue: " + err.Error())
		}
		if err := conn.Open(); err != nil {
			panic("failed to open queue: " + err.Error())
		}

		for queueName, qcfg := range m.queues {
			if qcfg.Connect == "" || qcfg.Connect == "*" || qcfg.Connect == name {
				realName := cfg.Prefix + queueName
				for i := 0; i < qcfg.Thread; i++ {
					if err := conn.Register(realName); err != nil {
						panic("failed to register queue: " + err.Error())
					}
				}
			}
		}

		inst.conn = conn
		m.instances[name] = inst
		if cfg.Weight > 0 {
			weights[name] = cfg.Weight
		}
	}

	m.weights = weights
	m.hashring = util.NewHashRing(weights)
	m.opened = true
}

func (m *Module) Start() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.started {
		return
	}
	for _, inst := range m.instances {
		if err := inst.conn.Start(); err != nil {
			panic("failed to start queue: " + err.Error())
		}
	}
	fmt.Printf("infrago queue module is running with %d connections, %d queues.\n", len(m.instances), len(m.queues))
	m.started = true
}

func (m *Module) Stop() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if !m.started {
		return
	}
	for _, inst := range m.instances {
		_ = inst.conn.Stop()
	}
	m.started = false
}

func (m *Module) Close() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if !m.opened {
		return
	}
	for _, inst := range m.instances {
		if inst.conn != nil {
			_ = inst.conn.Close()
			inst.conn = nil
		}
	}
	m.instances = make(map[string]*Instance, 0)
	m.weights = nil
	m.hashring = nil
	m.opened = false
}

func (m *Module) publish(connName, name string, value Map, delays ...time.Duration) error {
	if name == "" {
		return errInvalidQueue
	}

	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if connName == "" {
		if m.hashring == nil {
			return errNoConnection
		}
		connName = m.hashring.Locate(name)
	}

	inst, ok := m.instances[connName]
	if !ok || inst == nil || inst.conn == nil {
		return errNoConnection
	}

	if value == nil {
		value = Map{}
	}
	if dec, ok := m.declares[name]; ok && dec.Args != nil {
		mapped := Map{}
		res := infra.Mapping(dec.Args, value, mapped, dec.Nullable, false)
		if res == nil || res.OK() {
			value = mapped
		}
	}

	var data []byte
	if inst.Config.External {
		bytes, err := infra.Marshal(inst.Config.Codec, value)
		if err != nil {
			return err
		}
		data = bytes
	} else {
		body := msgEnvelope{
			Name:     name,
			Metadata: infra.NewMeta().Metadata(),
			Payload:  value,
		}
		bytes, err := infra.Marshal(inst.Config.Codec, body)
		if err != nil {
			return err
		}
		data = bytes
	}

	realName := inst.Config.Prefix + name
	if len(delays) > 0 {
		return inst.conn.DeferredPublish(realName, data, delays[0])
	}
	return inst.conn.Publish(realName, data)
}

func (inst *Instance) Submit(next func()) {
	go next()
}

func (inst *Instance) Serve(req Request) Response {
	name := req.Name
	if inst.Config.Prefix != "" && len(name) >= len(inst.Config.Prefix) && name[:len(inst.Config.Prefix)] == inst.Config.Prefix {
		name = name[len(inst.Config.Prefix):]
	}

	ctx := &Context{
		inst:    inst,
		Meta:    infra.NewMeta(),
		nexts:   make([]ctxFunc, 0),
		Setting: Map{},
		Value:   Map{},
		Args:    Map{},
		Locals:  Map{},
	}
	ctx.Name = name
	if cfg, ok := module.queues[name]; ok {
		ctx.Config = &cfg
		ctx.Setting = cfg.Setting
	}

	if ctx.Config != nil {
		retryCount := len(ctx.Config.Retry)
		ctx.attempt = req.Attempt
		if ctx.attempt <= 0 {
			ctx.attempt = 1
		}
		ctx.final = queueFinal(retryCount, ctx.attempt)
	}

	if inst.Config.External {
		payload := Map{}
		if err := infra.Unmarshal(inst.Config.Codec, req.Data, &payload); err == nil {
			ctx.Value = payload
		}
	} else {
		env := msgEnvelope{}
		if err := infra.Unmarshal(inst.Config.Codec, req.Data, &env); err == nil {
			ctx.Metadata(env.Metadata)
			if env.Payload != nil {
				ctx.Value = env.Payload
			}
			if env.Name != "" {
				ctx.Name = env.Name
			}
		}
	}

	span := ctx.Begin("queue:"+ctx.Name, infra.TraceAttrs("infrago", infra.TraceKindQueue, ctx.Name, Map{
		"module":     "queue",
		"connection": inst.Name,
		"operation":  "consume",
		"attempt":    ctx.Attempts(),
	}))

	inst.open(ctx)

	retry, delay := inst.responseMeta(ctx)
	if retry {
		span.End(infra.Retry)
	} else if res := ctx.Result(); res != nil && res.Fail() {
		span.End(res)
	} else {
		span.End()
	}
	return Response{Retry: retry, Delay: delay}
}

func (inst *Instance) responseMeta(ctx *Context) (bool, time.Duration) {
	if body, ok := ctx.Body.(retryBody); ok {
		if body.delay > 0 {
			return true, body.delay
		}
		return true, inst.nextRetryDelay(ctx)
	}

	if _, ok := ctx.Body.(time.Duration); ok {
		return true, ctx.Body.(time.Duration)
	}

	if res := ctx.Result(); infra.IsRetry(res) {
		return true, inst.nextRetryDelay(ctx)
	}

	return false, 0
}

func (inst *Instance) nextRetryDelay(ctx *Context) time.Duration {
	if ctx.Config == nil || len(ctx.Config.Retry) == 0 {
		return time.Second
	}
	idx := ctx.Attempts() - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(ctx.Config.Retry) {
		idx = len(ctx.Config.Retry) - 1
	}
	delay := ctx.Config.Retry[idx]
	if delay <= 0 {
		return time.Second
	}
	return delay
}

func queueFinal(retryCount, attempt int) bool {
	if retryCount <= 0 {
		return false
	}
	if attempt <= 0 {
		attempt = 1
	}
	return attempt > retryCount
}

func (inst *Instance) open(ctx *Context) {
	ctx.clear()
	ctx.next(module.serveFilters...)
	ctx.next(inst.serve)
	ctx.Next()
}

func (inst *Instance) serve(ctx *Context) {
	ctx.clear()
	ctx.next(module.requestFilters...)
	ctx.next(inst.request)
	ctx.Next()
	inst.response(ctx)
}

func (inst *Instance) request(ctx *Context) {
	ctx.clear()
	ctx.next(inst.finding)
	ctx.next(inst.authorizing)
	ctx.next(inst.arguing)
	ctx.next(inst.execute)
	ctx.Next()
}

func (inst *Instance) execute(ctx *Context) {
	ctx.clear()
	ctx.next(module.executeFilters...)
	if ctx.Config != nil {
		if len(ctx.Config.Actions) > 0 {
			ctx.next(ctx.Config.Actions...)
		}
		if ctx.Config.Action != nil {
			ctx.next(ctx.Config.Action)
		}
	}
	ctx.Next()
}

func (inst *Instance) response(ctx *Context) {
	ctx.clear()
	ctx.next(module.responseFilters...)
	ctx.Next()
	inst.body(ctx)
}

func (inst *Instance) finding(ctx *Context) {
	if ctx.Config == nil {
		ctx.Found()
		return
	}
	ctx.Next()
}

func (inst *Instance) authorizing(ctx *Context) {
	ctx.Next()
}

func (inst *Instance) arguing(ctx *Context) {
	if ctx.Config != nil && ctx.Config.Args != nil {
		argsValue := Map{}
		res := infra.Mapping(ctx.Config.Args, ctx.Value, argsValue, ctx.Config.Nullable, false, ctx.Timezone())
		if res != nil && res.Fail() {
			ctx.Failed(res)
			return
		}
		for k, v := range argsValue {
			ctx.Args[k] = v
		}
	}
	ctx.Next()
}

func (inst *Instance) found(ctx *Context) {
	ctx.clear()
	if ctx.Config != nil && ctx.Config.Found != nil {
		ctx.next(ctx.Config.Found)
	}
	ctx.next(module.foundHandlers...)
	ctx.Next()
}

func (inst *Instance) error(ctx *Context) {
	ctx.clear()
	if ctx.Config != nil && ctx.Config.Error != nil {
		ctx.next(ctx.Config.Error)
	}
	ctx.next(module.errorHandlers...)
	ctx.Next()
}

func (inst *Instance) failed(ctx *Context) {
	ctx.clear()
	if ctx.Config != nil && ctx.Config.Failed != nil {
		ctx.next(ctx.Config.Failed)
	}
	ctx.next(module.failedHandlers...)
	ctx.Next()
}

func (inst *Instance) denied(ctx *Context) {
	ctx.clear()
	if ctx.Config != nil && ctx.Config.Denied != nil {
		ctx.next(ctx.Config.Denied)
	}
	ctx.next(module.deniedHandlers...)
	ctx.Next()
}

func (inst *Instance) body(ctx *Context) {
	if ctx.Body == nil {
		if res := ctx.Result(); infra.IsRetry(res) {
			ctx.Body = retryBody{}
		} else {
			ctx.Body = finishBody{}
		}
		return
	}
	if d, ok := ctx.Body.(time.Duration); ok {
		ctx.Body = retryBody{delay: d}
	}
}
