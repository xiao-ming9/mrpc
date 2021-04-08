package client

import (
	"context"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	opentracinglog "github.com/opentracing/opentracing-go/log"
	"log"
	"mrpc/share/metadata"
	"mrpc/share/trace"
)

type OpenTracingWrapper struct {
	defaultClientWrapper
}

func NewOpenTracingWrapper() Wrapper {
	return &OpenTracingWrapper{}
}

// 同步调用支持
func (w *OpenTracingWrapper) WrapCall(option *SGOption, callFunc CallFunc) CallFunc {
	return func(ctx context.Context, ServiceMethod string, arg, reply interface{}) error {
		var clientSpan opentracing.Span
		// 只有不是心跳的才需要进行追踪
		if ServiceMethod != "" {
			var parentCtx opentracing.SpanContext
			// 先从当前 context 获取已经存在的追踪信息
			if parentSpan := opentracing.SpanFromContext(ctx); parentSpan != nil {
				parentCtx = parentSpan.Context()
			}
			// 开始埋点
			clientSpan = opentracing.StartSpan(
				ServiceMethod,
				opentracing.ChildOf(parentCtx),
				ext.SpanKindRPCClient)
			defer clientSpan.Finish()

			meta := metadata.FromContext(ctx)
			writer := &trace.MetaDataCarrier{
				Meta: &meta,
			}

			// 将追踪信息注入的 metadata 中，通过 rpc 传递下游
			err := clientSpan.Tracer().Inject(clientSpan.Context(), opentracing.TextMap, writer)
			if err != nil {
				log.Printf("inject trace error: %v", err)
			}
			ctx = metadata.WithMeta(ctx, meta)
		}

		err := callFunc(ctx, ServiceMethod, arg, reply)
		if err != nil {
			clientSpan.LogFields(opentracinglog.String("error", err.Error()))
		}
		return err
	}
}
