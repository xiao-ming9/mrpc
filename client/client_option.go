package client

import (
	"github.com/xiao-ming9/mrpc/codec"
	"github.com/xiao-ming9/mrpc/protocol"
	"github.com/xiao-ming9/mrpc/registry"
	"github.com/xiao-ming9/mrpc/selector"
	"github.com/xiao-ming9/mrpc/transport"
	"math"
	"time"
)

type Option struct {
	ProtocolType  protocol.ProtocolType
	SerializeType codec.SerializeType
	CompressType  protocol.CompressType
	TransportType transport.TransportType

	DialTimeout    time.Duration
	RequestTimeout time.Duration

	Heartbeat                 bool
	HeartbeatInterval         time.Duration
	HeartbeatDegradeThreshold int

	// 基于标签的路由策略
	Tagged bool
	Tags   map[string]string
}

var DefaultOption = Option{
	ProtocolType:  protocol.Default,
	SerializeType: codec.MessagePack,
	CompressType:  protocol.CompressTypeNone,
	TransportType: transport.TCPTransport,

	Heartbeat:                 false,
	HeartbeatInterval:         0,
	HeartbeatDegradeThreshold: math.MaxInt32,
}

type FailMode byte

const (
	FailFast  FailMode = iota // 快速失败
	FailOver                  // 重试其他服务器
	FailRetry                 // 重试同一个服务器
	FailSafe                  // 忽略失败，直接返回
)

type SGOption struct {
	AppKey       string
	RemoteAppkey string
	FailMode     FailMode
	Retries      int
	Registry     registry.Registry
	Selector     selector.Selector
	SelectOption selector.SelectOption
	Wrappers     []Wrapper

	Option
	Auth                    string
	CircuitBreakerThreshold uint64
	CircuitBreakerWindow    time.Duration
	Meta                    map[string]string
}

func AddWrapper(o *SGOption, w ...Wrapper) *SGOption {
	o.Wrappers = append(o.Wrappers, w...)
	return o
}

var DefaultSGOption = SGOption{
	AppKey:   "",
	FailMode: FailFast,
	Retries:  0,
	Selector: selector.NewRandomSelector(),

	Option: DefaultOption,

	Meta: make(map[string]string),
}
