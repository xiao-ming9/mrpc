package main

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/docker/libkv/store"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
	jaegerlog "github.com/uber/jaeger-client-go/log"
	"github.com/uber/jaeger-lib/metrics"
	"github.com/vmihailenco/msgpack"
	"github.com/xiao-ming9/mrpc/client"
	"github.com/xiao-ming9/mrpc/codec"
	"github.com/xiao-ming9/mrpc/protocol"
	"github.com/xiao-ming9/mrpc/registry/libkv"
	"github.com/xiao-ming9/mrpc/server"
	"github.com/xiao-ming9/mrpc/service"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"time"
)

const callTimes = 1

var s1, s2, s3 server.RpcServer

func main() {

	// 连接到 jaeger 追踪系统
	//log.Println("--------------------框架功能测试：链路追踪连接--------------------")
	cfg := config.Configuration{
		Sampler: &config.SamplerConfig{
			Type:  jaeger.SamplerTypeConst,
			Param: 1,
		},
		Reporter: &config.ReporterConfig{
			LogSpans: true,
		},
	}

	jLogger := jaegerlog.StdLogger
	jMetricsFactory := metrics.NullFactory

	_, err := cfg.InitGlobalTracer(
		"mrpc-service",
		config.Logger(jLogger),
		config.Metrics(jMetricsFactory),
	)
	if err != nil {
		log.Printf("Could not initialize jaeger tracer: %s", err.Error())
		return
	}

	StartServer()
	time.Sleep(2e10)

	start := time.Now()
	for i := 0; i < callTimes; i++ {
		MakeCall(codec.MessagePack)
	}
	cost := time.Now().Sub(start)
	log.Printf("rpc messagepack cost:%s", cost)

	start = time.Now()
	for i := 0; i < callTimes; i++ {
		MakeCall(codec.GOB)
	}
	cost = time.Now().Sub(start)
	log.Printf("rpc gob codec cost: %s", cost)

	//log.Println("--------------------框架功能测试：HTTP 调用--------------------")
	start = time.Now()
	for i := 0; i < callTimes; i++ {
		MakeHttpCall()
	}
	cost = time.Now().Sub(start)
	log.Printf("http call const: %s", cost)

	//log.Println("--------------------框架功能测试：服务关闭功能--------------------")

	StopServer()
}

//var Registry = memory.NewInMemoryRegistry()

//var Registry = zookeeper.NewZookeeperRegistry("my-app", "xzm/mrpc/service", []string{"127.0.0.1:2181"},
//	1e10, nil)

var Registry = libkv.NewKVRegistry(store.ZK, []string{"127.0.0.1:2181"}, "my-app",
	"xzm/mrpc/service", 1e10, nil)

// StartServer 服务器启动
func StartServer() {
	// 功能测试点一：服务多节点注册
	//log.Println("--------------------框架功能测试：服务多节点注册发布--------------------")
	go func() {
		serveOpt := server.DefaultOption
		serveOpt.RegisterOption.AppKey = "my-app"
		//log.Println("--------------------框架功能测试：注册中心--------------------")
		serveOpt.Registry = Registry
		serveOpt.Tags = map[string]string{"status": "alive"}

		s1 = server.NewRPCServer(serveOpt)
		err := s1.Register(service.Arith{})
		if err != nil {
			log.Println("err!!!" + err.Error())
		}
		port := 8880
		s1.Serve("tcp", ":"+strconv.Itoa(port), nil)
	}()

	go func() {
		serverOpt := server.DefaultOption
		serverOpt.RegisterOption.AppKey = "my-app"
		serverOpt.Registry = Registry
		serverOpt.Tags = map[string]string{"status": "alive"}

		s2 = server.NewRPCServer(serverOpt)
		err := s2.Register(service.Arith{})
		if err != nil {
			log.Println("err!!!" + err.Error())
		}
		port := 8881
		s2.Serve("tcp", ":"+strconv.Itoa(port), nil)
	}()

	go func() {
		serverOpt := server.DefaultOption
		serverOpt.RegisterOption.AppKey = "my-app"
		serverOpt.Registry = Registry
		serverOpt.Tags = map[string]string{"status": "alive"}

		s3 = server.NewRPCServer(serverOpt)
		err := s3.Register(service.Arith{})
		if err != nil {
			log.Println("err!!!" + err.Error())
		}
		port := 8882
		s3.Serve("tcp", ":"+strconv.Itoa(port), nil)
	}()
}

func MakeCall(t codec.SerializeType) {
	c := getSGClient(t)
	args := service.Args{
		A: rand.Intn(200),
		B: rand.Intn(100),
	}
	reply := &service.Reply{}
	ctx := context.Background()
	//log.Printf("--------------------框架功能测试：服务调用功能，包括负载均衡，容错处理，标签路由等---------------------")
	err := c.Call(ctx, "Arith.Add", args, reply)
	if err != nil {
		log.Println("err!!!" + err.Error())
	} else if reply.C != args.A+args.B {
		log.Printf("%d + %d != %d", args.A, args.B, reply.C)
	}

	args = service.Args{A: rand.Intn(200), B: rand.Intn(100)}
	reply = &service.Reply{}
	ctx = context.Background()
	err = c.Call(ctx, "Arith.Minus", args, reply)
	if err != nil {
		log.Println("err!!! " + err.Error())
	} else if reply.C != args.A-args.B {
		log.Printf("%d - %d != %d", args.A, args.B, reply.C)
	}

	args = service.Args{A: rand.Intn(200), B: rand.Intn(100)}
	reply = &service.Reply{}
	ctx = context.Background()
	err = c.Call(ctx, "Arith.Mul", args, reply)
	if err != nil {
		log.Println("err!!!" + err.Error())
	} else if reply.C != args.A*args.B {
		log.Printf("%d * %d != %d", args.A, args.B, reply.C)
	}

	args = service.Args{A: rand.Intn(200), B: rand.Intn(100)}
	reply = &service.Reply{}
	ctx = context.Background()
	err = c.Call(ctx, "Arith.Divide", args, reply)
	if args.B == 0 && err == nil {
		log.Println("err!!! didn't return errror!")
	} else if err != nil && err.Error() == "divided by 0" {
		log.Println(err.Error())
	} else if err != nil {
		log.Println("err!!!" + err.Error())
	} else if reply.C != args.A/args.B {
		log.Printf("%d / %d != %d", args.A, args.B, reply.C)
	}
}

func getSGClient(t codec.SerializeType) client.SGClient {
	op := &client.DefaultSGOption
	op.AppKey = "my-app"
	op.SerializeType = t
	op.RequestTimeout = time.Millisecond * 100
	op.DialTimeout = time.Millisecond * 100
	op.FailMode = client.FailRetry
	op.Retries = 3

	op.Heartbeat = true
	op.HeartbeatInterval = time.Second * 10
	op.HeartbeatDegradeThreshold = 10
	//log.Println("--------------------框架功能测试：标签功能--------------------")
	op.Tagged = true
	op.Tags = map[string]string{"status": "alive"}
	op.Registry = Registry
	//log.Println("--------------------框架功能测试：客户端各项拦截器功能，包括元数据处理，" +
	//	"日志记录，链路追踪，限流--------------------")
	// 性能测试时关闭限流
	client.AddWrapper(op, client.NewMetaDataWrapper(), client.NewLogWrapper())
	c := client.NewSGClient(*op)
	return c
}

func MakeHttpCall() {
	// 声明参数并序列化，放到 http 请求的 body 中
	arg := service.Args{
		A: rand.Intn(200),
		B: rand.Intn(100),
	}
	data, _ := msgpack.Marshal(arg)
	body := bytes.NewBuffer(data)
	req, err := http.NewRequest("POST", "http://localhost:5081/invoke", body)
	if err != nil {
		log.Printf("http new request err: %v", err)
		return
	}
	req.Header.Set(server.HEADER_SEQ, "1")
	req.Header.Set(server.HEADER_MESSAGE_TYPE, protocol.MessageTypeRequest.String())
	req.Header.Set(server.HEADER_COMPRESS_TYPE, protocol.CompressTypeNone.String())
	req.Header.Set(server.HEADER_SERIALIZE_TYPE, codec.MessagePack.String())
	req.Header.Set(server.HEADER_STATUS_CODE, protocol.StatusOK.String())
	req.Header.Set(server.HEADER_SERVICE_NAME, "Arith")
	req.Header.Set(server.HEADER_METHOD_NAME, "Add")
	req.Header.Set(server.HEADER_ERROR, "")
	meta := map[string]interface{}{"key": "value"}
	metaJson, _ := json.Marshal(meta)
	req.Header.Set(server.HEADER_META_DATA, string(metaJson))
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("http request err: %v", err)
		return
	}
	if response.StatusCode != http.StatusOK {
		log.Printf("response status code is not 200,is %v,the response data: %v", response.StatusCode, response)
	} else if response.Header.Get(server.HEADER_ERROR) != "" {
		log.Printf("response header error: %v", response.Header.Get(server.HEADER_ERROR))
	} else {
		data, err = ioutil.ReadAll(response.Body)
		result := service.Reply{}
		_ = msgpack.Unmarshal(data, &result)
		log.Printf("HTTP call Arith.Add,request arg.A= %d, arg.B= %d,response reply.C= %d\n", arg.A, arg.B, result.C)
	}
}

func StopServer() {
	s1.Close()
	s2.Close()
	s3.Close()
}
