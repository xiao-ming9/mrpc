package server

import (
	"github.com/xiao-ming9/mrpc/codec"
	"github.com/xiao-ming9/mrpc/protocol"
	"github.com/xiao-ming9/mrpc/registry"
	"github.com/xiao-ming9/mrpc/transport"
	"time"
)

// ShutDownHook 钩子方法
type ShutDownHook func(s *SGServer)

type Option struct {
	AppKey         string
	Registry       registry.Registry
	RegisterOption registry.RegisterOption
	Wrappers       []Wrapper
	ShutDownWait   time.Duration
	ShutDownHooks  []ShutDownHook
	Tags           map[string]string

	ProtocolType  protocol.ProtocolType
	SerializeType codec.SerializeType
	CompressType  protocol.CompressType
	TransportType transport.TransportType
}

var DefaultOption = Option{
	ShutDownWait:  time.Second * 12,
	ProtocolType:  protocol.Default,
	SerializeType: codec.MessagePack,
	CompressType:  protocol.CompressTypeNone,
	TransportType: transport.TCPTransport,
}
