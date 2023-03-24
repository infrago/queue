package queue

import (
	"time"

	. "github.com/infrago/base"
	"github.com/infrago/infra"
)

type (
	Delay = []time.Duration
	Retry = []time.Duration

	Queue struct {
		Alias    []string `json:"alias"`
		Name     string   `json:"name"`
		Text     string   `json:"text"`
		Nullable bool     `json:"-"`
		Args     Vars     `json:"args"`
		Setting  Map      `json:"-"`
		Coding   bool     `json:"-"`

		Action  ctxFunc   `json:"-"`
		Actions []ctxFunc `json:"-"`

		// 路由单独可定义的处理器
		Found  ctxFunc `json:"-"`
		Error  ctxFunc `json:"-"`
		Failed ctxFunc `json:"-"`
		Denied ctxFunc `json:"-"`

		Connect string `json:"connect"`

		//Option
		Thread int             `json:"thread"`
		Retry  []time.Duration `json:"delay"`
	}

	// Declare 声明，表示当前节点会发出的队列声明
	Declare struct {
		Alias    []string `json:"alias"`
		Name     string   `json:"name"`
		Text     string   `json:"text"`
		Nullable bool     `json:"-"`
		Args     Vars     `json:"args"`
	}

	Request struct {
		Name      string
		Data      []byte
		Attempt   int
		Timestamp time.Time
	}

	Response struct {
		Retry bool
		Delay time.Duration
	}

	// Filter 拦截器
	Filter struct {
		Name     string  `json:"name"`
		Text     string  `json:"text"`
		Serve    ctxFunc `json:"-"`
		Request  ctxFunc `json:"-"`
		Execute  ctxFunc `json:"-"`
		Response ctxFunc `json:"-"`
	}
	// Handler 处理器
	Handler struct {
		Name   string  `json:"name"`
		Text   string  `json:"text"`
		Found  ctxFunc `json:"-"`
		Error  ctxFunc `json:"-"`
		Failed ctxFunc `json:"-"`
		Denied ctxFunc `json:"-"`
	}

	Info struct {
		Name   string
		Thread int
		Retry  []time.Duration
	}
)

// 直接使用另外注册，是为了方便alias被替换
// 要不然有可能会重名，在别名里重名
func (module *Module) Queue(name string, config Queue) {
	if config.Thread <= 1 {
		config.Thread = 1
	}

	alias := make([]string, 0)
	if name != "" {
		alias = append(alias, name)
	}
	if config.Alias != nil {
		alias = append(alias, config.Alias...)
	}
	module.mutex.Lock()
	for _, key := range alias {
		if infra.Override() {
			module.queues[key] = config
		} else {
			if _, ok := module.queues[key]; ok == false {
				module.queues[key] = config
			}
		}
	}
	module.mutex.Unlock()

	//自动注册Declare
	module.Declare(name, Declare{config.Alias, config.Name, config.Text, config.Nullable, config.Args})
}

// Declare 声明
func (module *Module) Declare(name string, config Declare) {
	alias := make([]string, 0)
	if name != "" {
		alias = append(alias, name)
	}
	if config.Alias != nil {
		alias = append(alias, config.Alias...)
	}

	module.mutex.Lock()
	for _, key := range alias {
		if infra.Override() {
			module.declares[key] = config
		} else {
			if _, ok := module.declares[key]; ok == false {
				module.declares[key] = config
			}
		}
	}
	module.mutex.Unlock()
}

// Filter 注册 拦截器
func (module *Module) Filter(name string, config Filter) {
	if infra.Override() {
		module.filters[name] = config
	} else {
		if _, ok := module.filters[name]; ok == false {
			module.filters[name] = config
		}
	}
}

// Handler 注册 处理器
func (module *Module) Handler(name string, config Handler) {
	if infra.Override() {
		module.handlers[name] = config
	} else {
		if _, ok := module.handlers[name]; ok == false {
			module.handlers[name] = config
		}
	}
}
