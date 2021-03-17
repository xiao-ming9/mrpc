package client

import (
	"context"
	"errors"
	"io"
	"log"
	"mrpc/codec"
	"mrpc/protocol"
	"mrpc/transport"
	"strings"
	"sync"
	"sync/atomic"
)

var ErrorShutdown = errors.New("client is shut down")

type RPCClient interface {
	// Go 异步调用的方法
	Go(ctx context.Context, serviceMethod string, args, reply interface{}, done chan *Call) *Call
	// Call 同步调用的方法
	Call(ctx context.Context, serviceMethod string, args, reply interface{}) error
	Close() error
	IsShutDown() bool
}

type Call struct {
	ServiceMethod string      // 服务名，方法名
	Args          interface{} // 参数
	Reply         interface{} // 返回值（指针类型）
	Error         error       // 错误信息
	Done          chan *Call  // 在调用结束时激活
}

func (c *Call) done() {
	c.Done <- c
}

type simpleClient struct {
	codec        codec.Codec
	rwc          io.ReadWriteCloser
	pendingCalls sync.Map
	mutex        sync.Mutex
	shutdown     bool
	option       Option
	seq          uint64
}

func NewRPCClient(network, addr string, option Option) (RPCClient, error) {
	client := new(simpleClient)
	client.option = option
	client.codec = codec.GetCodec(option.SerializeType)

	tr := transport.NewTransport(option.TransportType)
	// TODO:第三个参数
	err := tr.Dial(network, addr, transport.DialOption{Timeout: option.DialTimeout})
	if err != nil {
		return nil, err
	}

	client.rwc = tr

	go client.input()
	return client, nil
}

// Go 异步调用的方法，其功能主要是组装参数
func (c *simpleClient) Go(ctx context.Context, serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	call := new(Call)
	call.ServiceMethod = serviceMethod
	call.Args = args
	call.Reply = reply

	if done == nil {
		done = make(chan *Call, 10) // buffered
	} else {
		// TODO: 测试一下超过10个是否会panic
		if cap(done) == 0 {
			log.Panic("rpc: done channel is unbuffered")
		}
	}
	call.Done = done

	c.send(ctx, call)

	return call
}

// Call 同步调用的方法，直接调用 Go 方法并阻塞等待知道返回或者超时
func (c *simpleClient) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	seq := atomic.AddUint64(&c.seq, 1)
	ctx = context.WithValue(ctx, protocol.RequestSeqKey, seq)

	//cancelFun := func() {}
	//if c.option.RequestTimeout != time.Duration(0) {
	//	ctx, cancelFun = context.WithTimeout(ctx, c.option.RequestTimeout)
	//	metaDataInterface := ctx.Value(protocol.MetaDataKey)
	//	var metaData map[string]string
	//	if metaDataInterface == nil {
	//		metaData = make(map[string]string)
	//	} else {
	//		metaData = metaDataInterface.(map[string]string)
	//	}
	//	metaData[protocol.RequestTimeoutKey] = c.option.RequestTimeout.String()
	//	ctx = context.WithValue(ctx, protocol.MetaDataKey, metaData)
	//}

	done := make(chan *Call, 1)
	call := c.Go(ctx, serviceMethod, args, reply, done)
	select {
	case <-ctx.Done():
		c.pendingCalls.Delete(seq)
		call.Error = errors.New("client request time out")
	case <-call.Done:
	}
	return call.Error
}

func (c *simpleClient) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.shutdown = true

	c.pendingCalls.Range(func(key, value interface{}) bool {
		call, ok := value.(*Call)
		if ok {
			call.Error = ErrorShutdown
			call.done()
		}

		c.pendingCalls.Delete(key)
		return true
	})
	return nil
}

func (c *simpleClient) IsShutDown() bool {
	return c.shutdown
}

// input 在 client 初始化完成时通过 go input() 执行。
// input 方法包含一个无限循环，在无限循环中读取传输层的数据并将其反序列化，并将反序列化得到的响应与缓存的请求进行匹配。
func (c *simpleClient) input() {
	var err error
	var response *protocol.Message
	for err == nil {
		response, err = protocol.DecodeMessage(c.option.ProtocolType, c.rwc)
		if err != nil {
			break
		}

		seq := response.Seq
		callInterface, ok := c.pendingCalls.Load(seq)
		if !ok {
			// 请求已经被清理掉了，可能是已经超时了
			continue
		}
		call := callInterface.(*Call)
		have := response.ServiceName + "." + response.MethodName
		want := call.ServiceMethod
		if have != want {
			log.Fatalf("servicemethod not equal! have:%s,want:%s", have, want)
		}
		c.pendingCalls.Delete(seq)

		switch {
		case response.Error != "":
			call.Error = ServiceError(response.Error)
			call.done()
		default:
			err = c.codec.Decode(response.Data, call.Reply)
			if err != nil {
				call.Error = errors.New("reading body " + err.Error())
			}
			call.done()
		}
	}
	log.Println("input error,closing client,error:"+err.Error())
	c.Close()
}

// send 将参数序列化并通过传输层的接口发送出去，同时将请求缓存到 pendingCalls 中
func (c *simpleClient) send(ctx context.Context, call *Call) {
	seq := ctx.Value(protocol.RequestSeqKey).(uint64)

	c.pendingCalls.Store(seq, call)

	request := protocol.NewMessage(c.option.ProtocolType)
	request.Seq = seq
	request.MessageType = protocol.MessageTypeRequest

	serviceMethod := strings.SplitN(call.ServiceMethod, ".", 2)
	request.ServiceName = serviceMethod[0]
	request.MethodName = serviceMethod[1]
	request.SerializeType = c.option.SerializeType
	request.CompressType = c.option.CompressType
	if ctx.Value(protocol.MetaDataKey) != nil {
		request.MetaData = ctx.Value(protocol.MetaDataKey).(map[string]interface{})
	}

	requestData, err := c.codec.Encode(call.Args)
	if err != nil {
		log.Println("client encode error:" + err.Error())
		c.pendingCalls.Delete(seq)
		call.Error = err
		call.done()
		return
	}

	request.Data = requestData

	data := protocol.EncodeMessage(c.option.ProtocolType, request)
	// 发送请求
	_, err = c.rwc.Write(data)
	if err != nil {
		log.Println("client write error:" + err.Error())
		c.pendingCalls.Delete(seq)
		call.Error = err
		call.done()
		return
	}
}
