package main

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/vmihailenco/msgpack"
	"io/ioutil"
	"log"
	"math/rand"
	"mrpc/codec"
	"mrpc/protocol"
	"mrpc/server"
	"mrpc/service"
	"net/http"
	"testing"
	"time"
)

func BenchmarkMakeCallGOB(b *testing.B) {
	// Server的初始化
	StartServer()
	time.Sleep(2e10)
	// 服务端的初始化
	args := service.Args{
		A: rand.Intn(200),
		B: rand.Intn(100),
	}
	c := getSGClient(codec.GOB)
	reply := &service.Reply{}
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := c.Call(ctx, "Arith.Add", args, reply)
		if err != nil {
			b.Logf("c.Call error: %v", err)
			b.Fail()
		}
	}
	b.StopTimer()
	// 关闭Server
	StopServer()
}

func BenchmarkMakeCallMessagePack(b *testing.B) {
	// Server的初始化
	StartServer()
	time.Sleep(2e10)
	// 服务端的初始化
	args := service.Args{
		A: rand.Intn(200),
		B: rand.Intn(100),
	}
	c := getSGClient(codec.MessagePack)
	reply := &service.Reply{}
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := c.Call(ctx, "Arith.Add", args, reply)
		if err != nil {
			b.Logf("c.Call error: %v", err)
			b.Fail()
		}
	}
	b.StopTimer()
	// 关闭Server
	StopServer()
}

func BenchmarkMakeHttpCall(b *testing.B) {
	StartServer()
	time.Sleep(2e10)
	// 声明参数并序列化，放到 http 请求的 body 中
	arg := service.Args{
		A: rand.Intn(200),
		B: rand.Intn(100),
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, _ := msgpack.Marshal(arg)
		body := bytes.NewBuffer(data)
		req, err := http.NewRequest("POST", "http://localhost:5081/invoke", body)
		if err != nil {
			b.Errorf("http new request err: %v", err)
			b.Fail()
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
			b.Errorf("http request err: %v", err)
			b.Fail()
			return
		}
		if response.StatusCode != http.StatusOK {
			b.Errorf("response status code is not 200,is %v,the response data: %v", response.StatusCode, response)
			b.Fail()
		} else if response.Header.Get(server.HEADER_ERROR) != "" {
			b.Errorf("response header error: %v", response.Header.Get(server.HEADER_ERROR))
			b.Fail()
		} else {
			data, err = ioutil.ReadAll(response.Body)
			result := service.Reply{}
			_ = msgpack.Unmarshal(data, &result)
			log.Println(arg.A,arg.B,result.C)
		}
	}
	b.StopTimer()
	// 关闭Server
	StopServer()
}

func BenchmarkMakeCallGobParallel(b *testing.B) {
	// Server的初始化
	StartServer()
	time.Sleep(2e10)
	// 服务端的初始化
	args := service.Args{
		A: rand.Intn(200),
		B: rand.Intn(100),
	}
	c := getSGClient(codec.GOB)
	reply := &service.Reply{}
	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := c.Call(ctx, "Arith.Add", args, reply)
			if err != nil {
				b.Logf("c.Call error: %v", err)
				b.Fail()
			}
		}
	})
	b.StopTimer()
	StopServer()
}

//func BenchmarkMakeCallGOB(b *testing.B) {
//	StartServer()
//	time.Sleep(1e9)
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		MakeCall(codec.GOB)
//	}
//	b.StopTimer()
//	StopServer()
//}
//
//func BenchmarkMakeCallMSGP(b *testing.B) {
//	StartServer()
//	time.Sleep(1e9)
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		MakeCall(codec.MessagePack)
//	}
//	b.StopTimer()
//	StopServer()
//}
