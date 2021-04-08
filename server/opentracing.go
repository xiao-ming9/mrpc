package server

import (
	"context"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"log"
	"mrpc/protocol"
	"mrpc/share/metadata"
	"mrpc/share/trace"
	"mrpc/transport"
)

type OpenTracingWrapper struct {
	defaultServerWrapper
}

func (w *OpenTracingWrapper) WrapHandleRequest(s *SGServer, requestFunc HandleRequestFunc) HandleRequestFunc {
	return func(ctx context.Context, request *protocol.Message, response *protocol.Message, tr transport.Transport) {
		if protocol.MessageTypeHeartbeat != request.MessageType {
			// 从 meta 中提取要追踪的信息
			meta := metadata.FromContext(ctx)
			spanContext, err := opentracing.GlobalTracer().Extract(opentracing.TextMap, &trace.MetaDataCarrier{Meta: &meta})
			if err != nil && err != opentracing.ErrSpanContextNotFound {
				log.Printf("extract span from meta error: %v", err)
			}

			// 开始服务端埋点
			serverSpan := opentracing.StartSpan(
				request.ServiceName+"."+request.MethodName,
				ext.RPCServerOption(spanContext),
				ext.SpanKindRPCServer)
			defer serverSpan.Finish()
			ctx = opentracing.ContextWithSpan(ctx, serverSpan)
		}
		requestFunc(ctx, request, response, tr)
	}
}
