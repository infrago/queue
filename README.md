# queue

`queue` 是 infrago 的模块包。

## 安装

```bash
go get github.com/infrago/queue@latest
```

## 最小接入

```go
package main

import (
    _ "github.com/infrago/queue"
    "github.com/infrago/infra"
)

func main() {
    infra.Run()
}
```

## 配置示例

```toml
[queue]
driver = "default"
```

## 公开 API（摘自源码）

- `func (Queue) RegistryComponent() string`
- `func (Queues) RegistryComponent() string`
- `func (ctx *Context) Next()`
- `func (ctx *Context) Found()`
- `func (ctx *Context) Error(res Res)`
- `func (ctx *Context) Failed(res Res)`
- `func (ctx *Context) Denied(res Res)`
- `func (ctx *Context) Attempts() int`
- `func (ctx *Context) Final() bool`
- `func (ctx *Context) Finish()`
- `func (ctx *Context) Retry(delays ...time.Duration)`
- `func (d *defaultDriver) Connect(inst *Instance) (Connection, error)`
- `func (c *defaultConnection) Open() error  { return nil }`
- `func (c *defaultConnection) Close() error { return nil }`
- `func (c *defaultConnection) Register(name string) error`
- `func (c *defaultConnection) Start() error`
- `func (c *defaultConnection) Stop() error`
- `func (c *defaultConnection) Publish(name string, data []byte) error`
- `func (c *defaultConnection) DeferredPublish(name string, data []byte, delay time.Duration) error`
- `func Publish(name string, values ...Map) error`
- `func PublishTo(conn, name string, values ...Map) error`
- `func DeferredPublish(name string, value Map, delay time.Duration) error`
- `func DeferredPublishTo(conn, name string, value Map, delay time.Duration) error`
- `func RegisterDriver(name string, driver Driver)`
- `func RegisterConfig(name string, cfg Config)`
- `func RegisterConfigs(cfgs Configs)`
- `func (m *Module) RegisterQueue(name string, cfg Queue)`
- `func (m *Module) RegisterDeclare(name string, cfg Declare)`
- `func (m *Module) RegisterFilter(name string, cfg Filter)`
- `func (m *Module) RegisterHandler(name string, cfg Handler)`
- `func (m *Module) Register(name string, value Any)`
- `func (m *Module) RegisterQueues(prefix string, queues Queues)`
- `func (m *Module) RegisterDriver(name string, driver Driver)`
- `func (m *Module) RegisterConfig(name string, cfg Config)`
- `func (m *Module) RegisterConfigs(configs Configs)`
- `func (m *Module) Config(global Map)`
- `func (m *Module) Setup()`
- `func (m *Module) Open()`
- `func (m *Module) Start()`
- `func (m *Module) Stop()`

## 排错

- 模块未运行：确认空导入已存在
- driver 无效：确认驱动包已引入
- 配置不生效：检查配置段名是否为 `[queue]`
