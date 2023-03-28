package queue

import (
	"strings"
	"time"

	. "github.com/infrago/base"
	"github.com/infrago/infra"
)

func (this *Instance) newContext() *Context {
	return &Context{
		inst: this, index: 0, nexts: make([]ctxFunc, 0),
		Setting: Map{}, Value: Map{}, Args: Map{}, Locals: Map{},
	}
}

// parse 解析元数据
// 加入payload直解
func (this *Instance) parse(data []byte) (infra.Metadata, error) {
	metadata := infra.Metadata{}

	if this.Config.External {
		//外部直接解析
		payload := Map{}
		err := infra.Unmarshal(this.Config.Codec, data, &payload)
		if err == nil {
			metadata.Payload = payload
		}
	} else {
		//内部
		err := infra.Unmarshal(this.Config.Codec, data, &metadata)
		if err == nil {
		}
	}

	return metadata, nil
}

func (this *Instance) Submit(next func()) {
	module.pool.Submit(next)
}

func (this *Instance) Serve(req Request) Response {
	name := req.Name
	if strings.HasPrefix(name, this.Config.Prefix) {
		name = strings.TrimPrefix(name, this.Config.Prefix)
	}

	ctx := this.newContext()
	ctx.Name = name
	if cfg, ok := module.queues[ctx.Name]; ok {
		ctx.Config = &cfg
		ctx.Setting = cfg.Setting
	}

	// 解析元数据

	metadata, err := this.parse(req.Data)
	if err == nil {
		metadata.Attempts = req.Attempt
		retry := len(ctx.Config.Retry)
		if retry >= 0 && req.Attempt > retry {
			metadata.Final = true
		}
		ctx.Metadata(metadata)
		ctx.Value = metadata.Payload
	}

	//开始执行
	this.open(ctx)
	infra.CloseMeta(&ctx.Meta)
	this.close(ctx)

	//失败时返回错误+延迟
	//成功返回nil

	retry := false
	delay := time.Duration(0)

	if body, ok := ctx.Body.(retryBody); ok {
		retries := len(ctx.Config.Retry)
		if retries != 0 {
			if retries < 0 || retries >= req.Attempt {
				retry = true //-1表示无限重试
			}

			if retries <= 0 {
				delay = time.Second
			} else if retries == 1 {
				delay = ctx.Config.Retry[0]
			} else {
				if req.Attempt <= 1 {
					//第一次请求，重试pos应该是0
					delay = ctx.Config.Retry[0]
				} else {
					dls := len(ctx.Config.Retry)
					pos := (req.Attempt - 1) % dls
					delay = ctx.Config.Retry[pos]
				}
			}
			if body.delay > 0 {
				delay = body.delay
			}
		}
	}
	return Response{
		retry, delay,
	}
}

// ctx 收尾工作
func (this *Instance) close(ctx *Context) {
}

func (this *Instance) open(ctx *Context) {
	//清理执行线
	ctx.clear()

	//队列没有预处理？

	//serve拦截器
	ctx.next(module.serveFilters...)
	ctx.next(this.serve)

	//开始执行
	ctx.Next()
}

func (this *Instance) serve(ctx *Context) {
	//清理执行线
	ctx.clear()

	//队列没有预处理？

	//request拦截器
	ctx.next(module.requestFilters...)
	ctx.next(this.request)

	//开始执行
	ctx.Next()

	//response得在这里
	//这样的话，requestFilter不能包含response
	//需要加入serve
	this.response(ctx)
}

// request 请求处理
func (this *Instance) request(ctx *Context) {
	ctx.clear()

	//request拦截器
	ctx.next(this.finding)     //存在否
	ctx.next(this.authorizing) //身份验证
	ctx.next(this.arguing)     //参数处理
	ctx.next(this.execute)

	//开始执行
	ctx.Next()
}

// execute 执行线
func (this *Instance) execute(ctx *Context) {
	ctx.clear()

	//execute拦截器
	ctx.next(module.executeFilters...)
	if ctx.Config.Actions != nil || len(ctx.Config.Actions) > 0 {
		ctx.next(ctx.Config.Actions...)
	}
	if ctx.Config.Action != nil {
		ctx.next(ctx.Config.Action)
	}

	//开始执行
	ctx.Next()
}

// response 响应线
func (this *Instance) response(ctx *Context) {
	ctx.clear()

	//response拦截器
	ctx.next(module.responseFilters...)

	//开始执行
	ctx.Next()

	//这样保证body一定会执行，要不然response不next就没法弄了
	this.body(ctx)
}

// finding 判断不
func (this *Instance) finding(ctx *Context) {
	if ctx.Config == nil {
		ctx.Found()
	} else {
		ctx.Next()
	}
}

// authorizing token验证
func (this *Instance) authorizing(ctx *Context) {
	// 待处理
	ctx.Next()
}

// arguing 参数解析
func (this *Instance) arguing(ctx *Context) {
	if ctx.Config.Args != nil {
		argsValue := Map{}
		res := infra.Mapping(ctx.Config.Args, ctx.Value, argsValue, ctx.Config.Nullable, false, ctx.Timezone())
		if res != nil && res.Fail() {
			ctx.Failed(res)
		}

		for k, v := range argsValue {
			ctx.Args[k] = v
		}
	}
	ctx.Next()
}

func (this *Instance) found(ctx *Context) {
	ctx.clear()

	//把处理器加入调用列表
	if ctx.Config.Found != nil {
		ctx.next(ctx.Config.Found)
	}
	ctx.next(module.foundHandlers...)

	ctx.Next()
}

func (this *Instance) error(ctx *Context) {
	ctx.clear()

	//把处理器加入调用列表
	if ctx.Config.Error != nil {
		ctx.next(ctx.Config.Error)
	}
	ctx.next(module.errorHandlers...)

	ctx.Next()
}

func (this *Instance) failed(ctx *Context) {
	ctx.clear()

	//把处理器加入调用列表
	if ctx.Config.Failed != nil {
		ctx.next(ctx.Config.Failed)
	}
	ctx.next(module.failedHandlers...)

	ctx.Next()
}

func (this *Instance) denied(ctx *Context) {
	ctx.clear()

	//把处理器加入调用列表
	if ctx.Config.Denied != nil {
		ctx.next(ctx.Config.Denied)
	}
	ctx.next(module.deniedHandlers...)

	ctx.Next()
}

// 最终的默认body响应
func (this *Instance) body(ctx *Context) {
	if ctx.Body == nil {
		res := ctx.Result()
		if res.Fail() {
			ctx.Body = retryBody{0}
		} else {
			ctx.Body = finishBody{}
		}
	} else {
		switch bbb := ctx.Body.(type) {
		case time.Duration:
			ctx.Body = retryBody{bbb}
		}
	}
}
