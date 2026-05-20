package queue

import (
	"context"
	"time"

	. "github.com/infrago/base"
	"github.com/infrago/infra"
)

type (
	Context struct {
		inst *Instance
		*infra.Meta
		context context.Context
		cancel  context.CancelFunc

		index int
		nexts []ctxFunc

		Name    string
		Config  *Queue
		Setting Map

		Value  Map
		Args   Map
		Locals Map

		Body Any

		attempt int
		final   bool
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

func (ctx *Context) Error(res Res) {
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

func (ctx *Context) Attempts() int {
	return ctx.attempt
}

func (ctx *Context) Final() bool {
	return ctx.final
}

func (ctx *Context) Context() context.Context {
	if ctx.context == nil {
		return context.Background()
	}
	return ctx.context
}

func (ctx *Context) Done() <-chan struct{} {
	return ctx.Context().Done()
}

func (ctx *Context) Finish() {
	ctx.Body = finishBody{}
}

func (ctx *Context) Retry(delays ...time.Duration) {
	delay := time.Duration(0)
	if len(delays) > 0 {
		delay = delays[0]
	}
	ctx.Body = retryBody{delay: delay}
}
