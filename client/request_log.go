package client

import (
	"context"
	"log"
)

// LogWrapper 该过滤器用于记录请求和响应
type LogWrapper struct {
	defaultClientWrapper
}

func NewLogWrapper() Wrapper {
	return &LogWrapper{}
}

func (l *LogWrapper) WrapCall(option *SGOption, callFunc CallFunc) CallFunc {
	return func(ctx context.Context, ServiceMethod string, arg, reply interface{}) error {
		log.Printf("before calling, ServiceMethod:%+v, arg:%+v", ServiceMethod, arg)
		err := callFunc(ctx, ServiceMethod, arg, reply)
		log.Printf("after calling,ServiceMethod:%+v,reply:%+v,error:%s", ServiceMethod, reply, err)
		return err
	}
}

func (l *LogWrapper) WrapGo(option *SGOption, goFunc GoFunc) GoFunc {
	return func(ctx context.Context, ServiceMethod string, arg, reply interface{}, done chan *Call) *Call {
		log.Printf("before going,ServiceMethod:%+v,arg:%+v", ServiceMethod, arg)
		return goFunc(ctx, ServiceMethod, arg, reply, done)
	}
}
