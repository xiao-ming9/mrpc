package client

import (
	"context"
)

// 客户端切面

type CallFunc func(ctx context.Context, ServiceMethod string, arg, reply interface{}) error
type GoFunc func(ctx context.Context, ServiceMethod string, arg, reply interface{}, done chan *Call) *Call

// 客户端拦截器

type Wrapper interface {
	WrapCall(option *SGOption, callFunc CallFunc) CallFunc
	WrapGo(option *SGOption, goFunc GoFunc) GoFunc
}

type defaultClientWrapper struct {
}

func (d *defaultClientWrapper) WrapCall(option *SGOption, callFunc CallFunc) CallFunc {
	return callFunc
}

func (d *defaultClientWrapper) WrapGo(option *SGOption, goFunc GoFunc) GoFunc {
	return goFunc
}
