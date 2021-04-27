package client

import (
	"context"
	"github.com/xiao-ming9/mrpc/protocol"
	"github.com/xiao-ming9/mrpc/share/metadata"
	"time"
)

// MetaDataWrapper 该过滤器用于封装元数据
type MetaDataWrapper struct {
	defaultClientWrapper
}

func NewMetaDataWrapper() *MetaDataWrapper {
	return &MetaDataWrapper{}
}

func (m *MetaDataWrapper) WrapCall(option *SGOption, callFunc CallFunc) CallFunc {
	return func(ctx context.Context, ServiceMethod string, arg, reply interface{}) error {
		ctx = wrapContext(ctx, option)
		return callFunc(ctx, ServiceMethod, arg, reply)
	}
}

func (m *MetaDataWrapper) WrapGo(option *SGOption, goFunc GoFunc) GoFunc {
	return func(ctx context.Context, ServiceMethod string, arg, reply interface{}, done chan *Call) *Call {
		ctx = wrapContext(ctx, option)
		return goFunc(ctx, ServiceMethod, arg, reply, done)
	}
}

// wrapContext 对 Context 根据 option 进行加工处理
func wrapContext(ctx context.Context, option *SGOption) context.Context {
	timeout := time.Duration(0)
	deadline, ok := ctx.Deadline()
	if ok {
		//已经有了deadline了，此次请求覆盖原始的超时时间
		timeout = time.Until(deadline)
	}

	if timeout == time.Duration(0) && option.RequestTimeout != time.Duration(0) {
		//ctx里没有设置超时时间，用option中设置的超时时间
		timeout = option.RequestTimeout
	}

	ctx, _ = context.WithTimeout(ctx, timeout)

	metaData := metadata.FromContext(ctx)
	metaData[protocol.RequestTimeoutKey] = uint64(timeout)

	if option.Auth != "" {
		metaData[protocol.AuthKey] = option.Auth
	}
	if auth, ok := ctx.Value(protocol.AuthKey).(string); ok {
		metaData[protocol.AuthKey] = auth
	}

	deadline, ok = ctx.Deadline()
	if ok {
		metaData[protocol.RequestDeadlineKey] = deadline.Unix()
	}
	ctx = metadata.WithMeta(ctx, metaData)
	return ctx
}
