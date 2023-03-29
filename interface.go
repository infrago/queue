package queue

import (
	"fmt"
	"log"
	"time"

	. "github.com/infrago/base"
	"github.com/infrago/infra"
	"github.com/infrago/util"

	"github.com/panjf2000/ants/v2"
)

func (this *Module) Register(name string, value Any) {
	switch config := value.(type) {
	case Driver:
		this.Driver(name, config)
	case Config:
		this.Config(name, config)
	case Configs:
		this.Configs(config)
	case Queue:
		this.Queue(name, config)
	case Declare:
		this.Declare(name, config)
	case Filter:
		this.Filter(name, config)
	case Handler:
		this.Handler(name, config)
	}
}

func (this *Module) configure(name string, config Map) {
	cfg := Config{
		Driver: infra.DEFAULT, Weight: 1, Codec: infra.GOB,
	}
	//如果已经存在了，用现成的改写
	if vv, ok := this.configs[name]; ok {
		cfg = vv
	}

	if driver, ok := config["driver"].(string); ok {
		cfg.Driver = driver
	}

	if external, ok := config["external"].(bool); ok {
		cfg.External = external
	}

	//分配权重
	if weight, ok := config["weight"].(int); ok {
		cfg.Weight = weight
	}
	if weight, ok := config["weight"].(int64); ok {
		cfg.Weight = int(weight)
	}
	if weight, ok := config["weight"].(float64); ok {
		cfg.Weight = int(weight)
	}

	if weight, ok := config["weight"].(float64); ok {
		cfg.Weight = int(weight)
	}

	if setting, ok := config["setting"].(Map); ok {
		cfg.Setting = setting
	}

	//保存配置
	this.configs[name] = cfg
}
func (this *Module) Configure(global Map) {
	var config Map
	if vvv, ok := global["queue"].(Map); ok {
		config = vvv
	}
	if config == nil {
		return
	}

	//记录上一层的配置，如果有的话
	rootConfig := Map{}

	for key, val := range config {
		if conf, ok := val.(Map); ok {
			this.configure(key, conf)
		} else {
			rootConfig[key] = val
		}
	}

	if len(rootConfig) > 0 {
		this.configure(infra.DEFAULT, rootConfig)
	}
}
func (this *Module) Initialize() {
	if this.initialized {
		return
	}

	pool, err := ants.NewPool(-1)
	if err != nil {
		panic("[队列]创建协程池失败：" + err.Error())
	}
	this.pool = pool

	// 如果没有配置任何连接时，默认一个
	if len(this.configs) == 0 {
		this.configs[infra.DEFAULT] = Config{
			Driver: infra.DEFAULT, Weight: 1, Codec: infra.GOB,
		}
	} else {
		// 默认分布， 如果想不参与分布，Weight设置为小于0 即可
		for key, config := range this.configs {
			if config.Weight == 0 {
				config.Weight = 1
			}
			if config.External {
				config.Weight = -1
			}
			this.configs[key] = config
		}
	}

	//默认队列参数
	for name, config := range this.queues {
		if config.Thread <= 0 {
			config.Thread = 1
		}
		this.queues[name] = config
	}

	//拦截器
	this.serveFilters = make([]ctxFunc, 0)
	this.requestFilters = make([]ctxFunc, 0)
	this.executeFilters = make([]ctxFunc, 0)
	this.responseFilters = make([]ctxFunc, 0)
	for _, filter := range this.filters {
		if filter.Serve != nil {
			this.serveFilters = append(this.serveFilters, filter.Serve)
		}
		if filter.Request != nil {
			this.requestFilters = append(this.requestFilters, filter.Request)
		}
		if filter.Execute != nil {
			this.executeFilters = append(this.executeFilters, filter.Execute)
		}
		if filter.Response != nil {
			this.responseFilters = append(this.responseFilters, filter.Response)
		}
	}

	//处理器
	this.foundHandlers = make([]ctxFunc, 0)
	this.errorHandlers = make([]ctxFunc, 0)
	this.failedHandlers = make([]ctxFunc, 0)
	this.deniedHandlers = make([]ctxFunc, 0)
	for _, filter := range this.handlers {
		if filter.Found != nil {
			this.foundHandlers = append(this.foundHandlers, filter.Found)
		}
		if filter.Error != nil {
			this.errorHandlers = append(this.errorHandlers, filter.Error)
		}
		if filter.Failed != nil {
			this.failedHandlers = append(this.failedHandlers, filter.Failed)
		}
		if filter.Denied != nil {
			this.deniedHandlers = append(this.deniedHandlers, filter.Denied)
		}
	}

	this.initialized = true
}
func (this *Module) Connect() {
	if this.connected {
		return
	}

	//记录要参与分布的连接和权重
	weights := make(map[string]int)

	for name, config := range this.configs {
		driver, ok := this.drivers[config.Driver]
		if ok == false {
			panic("Invalid queue driver: " + config.Driver)
		}

		inst := &Instance{
			nil, name, config, config.Setting,
		}

		// 建立连接
		connect, err := driver.Connect(inst)
		if err != nil {
			panic("Failed to connect to queue: " + err.Error())
		}

		// 打开连接
		err = connect.Open()
		if err != nil {
			panic("Failed to open queue connect: " + err.Error())
		}

		inst.connect = connect

		//注册队列
		for msgName, msgConfig := range this.queues {
			if msgConfig.Connect == "" || msgConfig.Connect == "*" || msgConfig.Connect == name {
				realName := config.Prefix + msgName
				// 注册队列
				for i := 0; i < msgConfig.Thread; i++ {
					if err := connect.Register(realName); err != nil {
						panic("Failed to register queue: " + err.Error())
					}
				}
			}
		}

		inst.connect = connect

		//保存实例
		this.instances[name] = inst

		if config.Weight > 0 {
			weights[name] = config.Weight //参与分布
		}
	}

	//hashring分片
	this.weights = weights
	this.hashring = util.NewHashRing(weights)

	this.connected = true
}
func (this *Module) Launch() {
	if this.launched {
		return
	}

	//全部开始来来来
	for _, inst := range this.instances {
		err := inst.connect.Start()
		if err != nil {
			panic("Failed to start queue: " + err.Error())
		}
	}

	log.Println(fmt.Sprintf("%s QUEUE is running with %d connects, %d queues.", infra.INFRAGO, len(this.instances), len(this.queues)))

	this.launched = true
}
func (this *Module) Terminate() {
	// 先停止订阅，不再接受新消息
	for _, ins := range this.instances {
		ins.connect.Stop()
	}

	//关闭协程池
	this.pool.ReleaseTimeout(time.Minute)

	// 关闭所有连接，
	for _, ins := range this.instances {
		ins.connect.Close()
	}

	this.launched = false
	this.connected = false
	this.initialized = false
}
