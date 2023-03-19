package queue

import (
	"time"

	. "github.com/infrago/base"
	"github.com/infrago/infra"
)

type (
	Context struct {
		inst *Instance
		infra.Meta

		index int       //下一个索引
		nexts []ctxFunc //方法列表

		// 以下几个字段必须独立
		// 要不然，Invoke的时候，会被修改掉
		Name    string
		Config  *Queue
		Setting Map

		Value  Map
		Args   Map
		Locals Map

		Body Any
	}
	ctxFunc func(*Context)

	finishBody struct{}
	retryBody  struct {
		delay time.Duration
	}
)

func (ctx *Context) clear() {
	ctx.index = 0
	ctx.nexts = make([]ctxFunc, 0)
}
func (ctx *Context) next(nexts ...ctxFunc) {
	ctx.nexts = append(ctx.nexts, nexts...)
}

func (ctx *Context) Next() {
	if len(ctx.nexts) > ctx.index {
		next := ctx.nexts[ctx.index]
		ctx.index++
		if next != nil {
			next(ctx)
		} else {
			ctx.Next()
		}
	}
}

func (ctx *Context) Found() {
	ctx.inst.found(ctx)
}
func (ctx *Context) Erred(res Res) {
	ctx.Result(res)
	ctx.inst.error(ctx)
}
func (ctx *Context) Failed(res Res) {
	ctx.Result(res)
	ctx.inst.failed(ctx)
}
func (ctx *Context) Denied(res Res) {
	ctx.Result(res)
	ctx.inst.denied(ctx)
}

func (ctx *Context) Finish() {
	ctx.Body = finishBody{}
}

// 返回延迟（相当于重试）
func (ctx *Context) Retry(delays ...time.Duration) {
	delay := time.Duration(0)
	if len(delays) > 0 {
		delay = delays[0]
	}
	ctx.Body = retryBody{delay}
}
