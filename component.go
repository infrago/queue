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
		Desc     string   `json:"desc"`
		Nullable bool     `json:"-"`
		Args     Vars     `json:"args"`
		Setting  Map      `json:"setting"`

		Action  ctxFunc   `json:"-"`
		Actions []ctxFunc `json:"-"`

		Found  ctxFunc `json:"-"`
		Error  ctxFunc `json:"-"`
		Failed ctxFunc `json:"-"`
		Denied ctxFunc `json:"-"`

		Connect string `json:"connect"`
		Thread  int    `json:"thread"`
		Retry   Retry  `json:"retry"`
	}

	Queues map[string]Queue

	Declare struct {
		Alias    []string `json:"alias"`
		Name     string   `json:"name"`
		Desc     string   `json:"desc"`
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

	Filter struct {
		Name     string  `json:"name"`
		Desc     string  `json:"desc"`
		Serve    ctxFunc `json:"-"`
		Request  ctxFunc `json:"-"`
		Execute  ctxFunc `json:"-"`
		Response ctxFunc `json:"-"`
	}

	Handler struct {
		Name   string  `json:"name"`
		Desc   string  `json:"desc"`
		Found  ctxFunc `json:"-"`
		Error  ctxFunc `json:"-"`
		Failed ctxFunc `json:"-"`
		Denied ctxFunc `json:"-"`
	}
)

func (m *Module) RegisterQueue(name string, cfg Queue) {
	if cfg.Thread <= 0 {
		cfg.Thread = 1
	}
	keys := collectAlias(name, cfg.Alias)

	m.mutex.Lock()
	for _, key := range keys {
		if infra.Override() {
			m.queues[key] = cfg
		} else if _, ok := m.queues[key]; !ok {
			m.queues[key] = cfg
		}
	}
	m.mutex.Unlock()

	m.RegisterDeclare(name, Declare{
		Alias:    cfg.Alias,
		Name:     cfg.Name,
		Desc:     cfg.Desc,
		Nullable: cfg.Nullable,
		Args:     cfg.Args,
	})
}

func (m *Module) RegisterDeclare(name string, cfg Declare) {
	keys := collectAlias(name, cfg.Alias)

	m.mutex.Lock()
	defer m.mutex.Unlock()
	for _, key := range keys {
		if infra.Override() {
			m.declares[key] = cfg
		} else if _, ok := m.declares[key]; !ok {
			m.declares[key] = cfg
		}
	}
}

func (m *Module) RegisterFilter(name string, cfg Filter) {
	if name == "" {
		return
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if infra.Override() {
		m.filters[name] = cfg
	} else if _, ok := m.filters[name]; !ok {
		m.filters[name] = cfg
	}
}

func (m *Module) RegisterHandler(name string, cfg Handler) {
	if name == "" {
		return
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if infra.Override() {
		m.handlers[name] = cfg
	} else if _, ok := m.handlers[name]; !ok {
		m.handlers[name] = cfg
	}
}

func collectAlias(name string, alias []string) []string {
	keys := make([]string, 0, 1+len(alias))
	if name != "" {
		keys = append(keys, name)
	}
	keys = append(keys, alias...)
	return keys
}
