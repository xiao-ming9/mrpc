package main

import (
	"context"
	"github.com/docker/libkv/store"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
	jaegerlog "github.com/uber/jaeger-client-go/log"
	"github.com/uber/jaeger-lib/metrics"
	"log"
	"math/rand"
	"mrpc/client"
	"mrpc/codec"
	"mrpc/registry/libkv"
	"mrpc/server"
	"mrpc/service"
	"mrpc/share/ratelimit"
	"strconv"
	"time"
)

const callTimes = 1

var s1, s2, s3 server.RpcServer

func main() {

	// 连接到 jaeger 追踪系统
	cfg := config.Configuration{
		Sampler: &config.SamplerConfig{
			Type:  jaeger.SamplerTypeConst,
			Param: 1,
		},
		Reporter: &config.ReporterConfig{
			LogSpans: true,
		},
	}

	// Example logger and metrics factory. Use github.com/uber/jaeger-client-go/log
	// and github.com/uber/jaeger-lib/metrics respectively to bind to real logging and metrics
	// frameworks.
	jLogger := jaegerlog.StdLogger
	jMetricsFactory := metrics.NullFactory

	// Initialize tracer with a logger and a metrics factory
	_, err := cfg.InitGlobalTracer(
		"mrpc-service",
		config.Logger(jLogger),
		config.Metrics(jMetricsFactory),
	)
	if err != nil {
		log.Printf("Could not initialize jaeger tracer: %s", err.Error())
		return
	}
	//defer closer.Close()


	StartServer()
	time.Sleep(2e9)
	start := time.Now()
	for i := 0; i < callTimes; i++ {
		MakeCall(codec.MessagePack)
	}
	cost := time.Now().Sub(start)
	log.Printf("cost:%s", cost)

	start = time.Now()
	for i := 0; i < callTimes; i++ {
		MakeCall(codec.GOB)
	}
	cost = time.Now().Sub(start)
	log.Printf("cost: %s", cost)

	//StopServer()

	time.Sleep(10*time.Minute)
}

//var Registry = memory.NewInMemoryRegistry()
//var Registry = zookeeper.NewZookeeperRegistry("my-app", "xzm/mrpc/service", []string{"127.0.0.1:2181"},
//	1e10, nil)
var Registry = libkv.NewKVRegistry(store.ZK, []string{"127.0.0.1:2181"}, "my-app",
	"xzm/mrpc/service", 1e10, nil)

func StartServer() {
	go func() {
		serveOpt := server.DefaultOption
		serveOpt.RegisterOption.AppKey = "my-app"
		serveOpt.Registry = Registry
		serveOpt.Tags = map[string]string{"status": "stopped"}

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
	op.Tagged = true
	op.Tags = map[string]string{"status": "alive"}
	op.Wrappers = append(op.Wrappers, &client.RateLimitWrapper{
		Limit: &ratelimit.DefaultRateLimiter{
			Num: 1,
		},
	})
	op.Registry = Registry

	c := client.NewSGClient(*op)

	args := service.Args{
		A: rand.Intn(200),
		B: rand.Intn(100),
	}
	reply := &service.Reply{}
	ctx := context.Background()
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
		log.Println("err!!!" + err.Error())
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

func StopServer() {
	s1.Close()
	s2.Close()
	s3.Close()
}
