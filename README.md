# queue

`queue` 是 infrago 的**模块**。

## 包定位

- 类型：模块
- 作用：队列模块，负责生产、消费、确认、重试流程。

## 主要功能

- 对上提供统一模块接口
- 对下通过驱动接口接入具体后端
- 支持按配置切换驱动实现

## 快速接入

```go
import _ "github.com/infrago/queue"
```

```toml
[queue]
driver = "default"
```

## 驱动实现接口列表

以下接口由驱动实现（来自模块 `driver.go`）：

### Driver

- `Connect(*Instance) (Connection, error)`

### Connection

- `Open() error`
- `Close() error`
- `Start() error`
- `Stop() error`
- `Register(name string) error`
- `Publish(name string, data []byte) error`
- `DeferredPublish(name string, data []byte, delay time.Duration) error`

## 全局配置项（所有配置键）

配置段：`[queue]`

- `driver`
- `external`
- `codec`
- `prefix`
- `weight`
- `setting`

## 说明

- `setting` 一般用于向具体驱动透传专用参数
- 默认内存驱动支持 `setting.buffer`，也可通过单个 Queue 的 `Setting["buffer"]` 覆盖
- 默认内存驱动支持 `setting.blocking_publish` 和 `setting.publish_timeout`
- Queue 的 `Setting["dead"]` / `dead_letter` / `dlq` 可配置最终失败消息转发队列
- 业务处理 panic 会被捕获为可重试失败，`Context.Context()` 可感知停止取消
- 多实例配置请参考模块源码中的 Config/configure 处理逻辑
