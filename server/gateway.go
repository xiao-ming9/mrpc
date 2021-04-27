// Package server
// http gateway 可以接收来自客户端的 http 请求，并将其转换为 rpc 请求然后交给服务端处理，再将服务端处理过后的结果通过 http 响应返回给客户端
package server

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"github.com/xiao-ming9/mrpc/codec"
	"github.com/xiao-ming9/mrpc/protocol"
	"github.com/xiao-ming9/mrpc/share/metadata"
	"net"
	"net/http"
	"strconv"
	"strings"
)

// 定义 http header 中的各个字段
const (
	HEADER_SEQ            = "rpc-header-seq"            // 序号，用来唯一标识请求或者响应
	HEADER_MESSAGE_TYPE   = "rpc-header-message_type"   // 消息类型，用来标识一个消息是请求还是响应
	HEADER_COMPRESS_TYPE  = "rpc-header-compress_type"  // 压缩类型，用来标识一个消息的压缩方式
	HEADER_SERIALIZE_TYPE = "rpc-header-serialize_type" // 序列化类型，用来标识消息体采用的编码方式
	HEADER_STATUS_CODE    = "rpc-header-status_code"    // 状态类型，用来标识一个请求是正常还是异常
	HEADER_SERVICE_NAME   = "rpc-header-service_name"   // 服务名
	HEADER_METHOD_NAME    = "rpc-header_method_name"    // 方法名
	HEADER_ERROR          = "rpc-header-error"          // 方法调用时发生的异常
	HEADER_META_DATA      = "rpc-header-meta_data"      // 其它元数据
)

// startGateway 启动一个 http server 用来接收 http 请求
func (s *SGServer) startGateway() {
	port := 5080
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	for err != nil && strings.Contains(err.Error(), "address already in use") {
		// 如果端口已经在使用，则递增端口
		port++
		ln, err = net.Listen("tcp", ":"+strconv.Itoa(port))
	}
	if err != nil {
		log.Printf("error listening gateway: %s", err.Error())
	}
	log.Printf("gateway listening on " + strconv.Itoa(port))
	// 避免阻塞，使用新的 goroutine 来执行 http server
	go func() {
		err := http.Serve(ln, s)
		if err != nil {
			log.Printf("error serving http %s", err.Error())
		}
	}()
}

func (s *SGServer) ServeHTTP(rw http.ResponseWriter, request *http.Request) {
	// 如果 url 不对则直接返回
	if request.URL.Path != "/invoke" {
		rw.WriteHeader(http.StatusNotFound)
		return
	}
	// 如果 method 不对则直接返回
	if request.Method != "POST" {
		rw.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// 构造新的请求
	rpcRequest := protocol.NewMessage(s.Option.ProtocolType)
	// 根据 http header 填充 request 的 header
	rpcRequest, err := parseHeader(rpcRequest, request)
	if err != nil {
		log.Printf("error parse http request header: %v", err)
		rw.WriteHeader(http.StatusBadRequest)
	}
	// 根据 http body 填充 request 的 data
	rpcRequest, err = parseBody(rpcRequest, request)
	if err != nil {
		log.Printf("error parse http request body: %v", err)
		rw.WriteHeader(http.StatusBadRequest)
	}

	// 构建 context
	ctx := metadata.WithMeta(context.Background(), rpcRequest.MetaData)
	response := rpcRequest.Clone()
	response.MessageType = protocol.MessageTypeResponse
	// 处理请求
	response = s.process(ctx, rpcRequest, response)
	// 返回响应
	s.writeHttpResponse(response, rw, request)
}

func parseHeader(message *protocol.Message, request *http.Request) (*protocol.Message, error) {
	headerSeq := request.Header.Get(HEADER_SEQ)
	seq, err := strconv.ParseUint(headerSeq, 10, 64)
	if err != nil {
		return nil, err
	}
	message.Seq = seq

	headerMsgType := request.Header.Get(HEADER_MESSAGE_TYPE)
	msgType, err := protocol.ParseMessageType(headerMsgType)
	if err != nil {
		return nil, err
	}
	message.MessageType = msgType

	headerCompressType := request.Header.Get(HEADER_COMPRESS_TYPE)
	compressType, err := protocol.ParseCompressType(headerCompressType)
	if err != nil {
		return nil, err
	}
	message.CompressType = compressType

	headerSerializeType := request.Header.Get(HEADER_SERIALIZE_TYPE)
	serializeType, err := codec.ParseSerializeType(headerSerializeType)
	if err != nil {
		return nil, err
	}
	message.SerializeType = serializeType

	headerStatusCode := request.Header.Get(HEADER_STATUS_CODE)
	statusCode, err := protocol.ParseStatusCode(headerStatusCode)
	if err != nil {
		return nil, err
	}
	message.StatusCode = statusCode

	serviceName := request.Header.Get(HEADER_SERVICE_NAME)
	message.ServiceName = serviceName

	methodName := request.Header.Get(HEADER_METHOD_NAME)
	message.MethodName = methodName

	errorMsg := request.Header.Get(HEADER_ERROR)
	message.Error = errorMsg

	headerMeta := request.Header.Get(HEADER_META_DATA)
	meta := make(map[string]interface{})
	err = json.Unmarshal([]byte(headerMeta), &meta)
	if err != nil {
		return nil, err
	}
	message.MetaData = meta

	return message, nil
}

func parseBody(message *protocol.Message, request *http.Request) (*protocol.Message, error) {
	data, err := ioutil.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	message.Data = data
	return message, nil
}

func (s *SGServer) writeHttpResponse(message *protocol.Message, rw http.ResponseWriter, request *http.Request) {
	header := rw.Header()
	header.Set(HEADER_SEQ, strconv.FormatUint(message.Seq, 10))
	header.Set(HEADER_MESSAGE_TYPE, message.MessageType.String())
	header.Set(HEADER_COMPRESS_TYPE, message.CompressType.String())
	header.Set(HEADER_SERIALIZE_TYPE, message.SerializeType.String())
	header.Set(HEADER_STATUS_CODE, message.StatusCode.String())
	header.Set(HEADER_SERVICE_NAME, message.ServiceName)
	header.Set(HEADER_METHOD_NAME, message.MethodName)
	header.Set(HEADER_ERROR, message.Error)
	metaDataJson, _ := json.Marshal(message.MetaData)
	header.Set(HEADER_META_DATA, string(metaDataJson))

	_, _ = rw.Write(message.Data)
}
