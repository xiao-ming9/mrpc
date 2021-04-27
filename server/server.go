package server

import (
	"context"
	"errors"
	"github.com/xiao-ming9/mrpc/codec"
	"github.com/xiao-ming9/mrpc/protocol"
	"github.com/xiao-ming9/mrpc/registry"
	"github.com/xiao-ming9/mrpc/share/metadata"
	"github.com/xiao-ming9/mrpc/transport"
	"io"
	"log"
	"reflect"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

type RpcServer interface {
	// Register 注册服务实例，receiver 是对外暴露的方法的实现者
	// metaData 是注册服务时携带的额外元数据，描述了 receiver 的其他信息
	Register(receiver interface{}) error

	// Serve 开始对外提供服务的接口
	Serve(network string, addr string, metaData map[string]interface{}) error
	Service() []ServiceInfo
	Close() error
}

type ServiceInfo struct {
	Name    string   `json:"name"`
	Methods []string `json:"methods"`
}

// SGServer 实现了 RPCServer 接口，SG(service governance)表示服务治理
type SGServer struct {
	codec            codec.Codec
	serviceMap       sync.Map
	tr               transport.ServerTransport
	mutex            sync.Mutex
	shutdown         bool
	RequestInProcess int64 // 表示当前正在处理中的请求

	Network string
	Addr    string

	Option Option
}

type methodType struct {
	method    reflect.Method
	ArgType   reflect.Type
	ReplyType reflect.Type
}

type service struct {
	name    string
	typ     reflect.Type
	rcvr    reflect.Value
	methods sync.Map
}

func NewRPCServer(option Option) RpcServer {
	s := new(SGServer)
	s.Option = option
	s.Option.Wrappers = append(s.Option.Wrappers,
		&DefaultServerWrapper{},
		&OpenTracingWrapper{},
	)
	s.AddShutdownHook(func(s *SGServer) {
		provider := registry.Provider{
			ProviderKey: s.Network + "@" + s.Addr,
			Network:     s.Network,
			Addr:        s.Addr,
		}
		s.Option.Registry.Unregister(s.Option.RegisterOption, provider)
		s.Close()
	})
	s.codec = codec.GetCodec(option.SerializeType)
	return s
}

func (s *SGServer) Register(receiver interface{}) error {
	recvTyp := reflect.TypeOf(receiver)
	name := recvTyp.Name()
	srv := new(service)
	srv.name = name
	srv.rcvr = reflect.ValueOf(receiver)
	srv.typ = recvTyp
	methods := suitableMethods(recvTyp, true)

	if len(methods) == 0 {
		var errorStr string

		// 如果对应的类型没有任何符合规则的方法，扫描对应的指针（参考 net.rpc 包）
		method := suitableMethods(reflect.PtrTo(srv.typ), false)
		if len(method) != 0 {
			errorStr = "mrpc.Register: type " + name + " has no exported methods of suitable type" +
				" (hint: pass a pointer to value of that type)"
		} else {
			errorStr = "mrpc.Register: type " + name + " has no exported methods of suitable type"
		}
		log.Println(errorStr)
		return errors.New(errorStr)
	}

	for k, v := range methods {
		srv.methods.LoadOrStore(k, v)
	}

	if _, duplicate := s.serviceMap.LoadOrStore(name, srv); duplicate {
		return errors.New("rpc: service already defined: " + name)
	}
	return nil
}

// 预计算反射类型是否有错误。无法直接使用错误，因为 Typeof 需要一个空接口值，需要事先进行转换
var typeOfError = reflect.TypeOf((*error)(nil)).Elem()
var typeOfContext = reflect.TypeOf((*context.Context)(nil)).Elem()

// suitableMethods 过滤符合规则的方法，参考 net.rpc 包
func suitableMethods(typ reflect.Type, reportErr bool) map[string]*methodType {
	methods := make(map[string]*methodType)
	// 循环过滤每一个方法
	for m := 0; m < typ.NumMethod(); m++ {
		method := typ.Method(m)
		mtype := method.Type
		mname := method.Name

		// 方法必须是可导出的
		if method.PkgPath != "" {
			continue
		}

		// 需要有四个参数：receiver，Context，args，*reply
		if mtype.NumIn() != 4 {
			if reportErr {
				log.Println("method", mname, " has wrong number of ins:", mtype.NumIn())
			}
			continue
		}

		// 第一个参数必须是context.Context
		ctxType := mtype.In(1)
		if !ctxType.Implements(typeOfContext) {
			if reportErr {
				log.Println("method", mname, " must use context.Context as the first parameter")
			}
			continue
		}

		// 第二个参数必须是 arg
		argType := mtype.In(2)
		if !isExportedOrBuiltinType(argType) {
			if reportErr {
				log.Println(mname, "parameter type not exported: ", argType)
			}
			continue
		}

		// 第三个参数是返回值，必须是指针类型
		replyType := mtype.In(3)
		if replyType.Kind() != reflect.Ptr {
			if reportErr {
				log.Println("method ", mname, " reply type not a pointer: ", replyType)
			}
			continue
		}

		// 返回值的类型必须是可导出的
		if !isExportedOrBuiltinType(replyType) {
			if reportErr {
				log.Println("method ", mname, " reply type not exported: ", replyType)
			}
			continue
		}

		// 只能有一个返回值
		if mtype.NumOut() != 1 {
			if reportErr {
				log.Println("method ", mname, " has wrong number of outs: ", mtype.NumOut())
			}
		}

		// 返回值的类型必须是 error
		if returnType := mtype.Out(0); returnType != typeOfError {
			if reportErr {
				log.Println("method ", mname, " returns ", returnType.String(), " not error")
			}
			continue
		}
		methods[mname] = &methodType{method: method, ArgType: argType, ReplyType: replyType}
	}
	return methods
}

// isExportedOrBuiltinType 判断参数是可导出的类型还是内建（不可导出）的类型
func isExportedOrBuiltinType(t reflect.Type) bool {
	// 使用循环以访问最底层的 type
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	// PkgPath will be non-empty even for an exported type,
	// so we need to check the type name as well.
	return isExported(t.Name()) || t.PkgPath() == ""
}

func isExported(name string) bool {
	r, _ := utf8.DecodeRuneInString(name)
	return unicode.IsUpper(r)
}

func (s *SGServer) Serve(network string, addr string, metaData map[string]interface{}) error {
	s.Network = network
	s.Addr = addr
	serveFunc := s.serve
	return s.wrapServe(serveFunc)(network, addr, metaData)
}

func (s *SGServer) wrapServe(serveFunc ServeFunc) ServeFunc {
	for _, w := range s.Option.Wrappers {
		serveFunc = w.WrapServe(s, serveFunc)
	}
	return serveFunc
}

func (s *SGServer) serve(network, addr string, meta map[string]interface{}) error {
	if s.shutdown {
		return nil
	}
	s.tr = transport.NewServerTransport(s.Option.TransportType)
	err := s.tr.Listen(network, addr)
	if err != nil {
		log.Printf("server listen on %s@%s error:%s\n", network, addr, err)
		return err
	}
	for {
		if s.shutdown {
			continue
		}
		conn, err := s.tr.Accept()
		if err != nil {
			if strings.Contains(err.Error(), "use of closed network connection") && s.shutdown {
				return nil
			}
			log.Printf("server accept on %s@%s error:%s\n", network, addr, err)
			return err
		}
		go s.serverTransport(conn)
	}
}

// serverTransport 在一个无限循环中从 Transport 读取数据并反序列化成请求，
// 根据请求指定的方法查找自身缓存的方法， 找到对应的方法后通过反射执行对应的实现并返回。
// 执行完成后再根据返回结果或者是执行发生的异常组装成一个完整的消息，通过Transport发送出去。
func (s *SGServer) serverTransport(tr transport.Transport) {
	for {
		if s.shutdown {
			tr.Close()
			continue
		}
		request, err := protocol.DecodeMessage(s.Option.ProtocolType, tr)

		if err != nil {
			if err == io.EOF {
				log.Printf("client has closed this connection: %s", tr.RemoteAddr().String())
			} else if strings.Contains(err.Error(), " use of closed network connection") {
				log.Printf("mrpc: connection %s is closed", tr.RemoteAddr().String())
			} else {
				log.Printf("mrpc: failed to read request: %v", err)
			}
			return
		}
		response := request.Clone()
		response.MessageType = protocol.MessageTypeResponse

		deadline, ok := response.Deadline()
		ctx := metadata.WithMeta(context.Background(), response.MetaData)
		if ok {
			ctx, _ = context.WithDeadline(ctx, deadline)
		}

		handleFunc := s.doHandleRequest
		s.wrapHandleRequest(handleFunc)(ctx, request, response, tr)
	}
}

func (s *SGServer) doHandleRequest(ctx context.Context, request *protocol.Message, response *protocol.Message, tr transport.Transport) {
	response = s.process(ctx, request, response)
	s.writeResponse(ctx, tr, response)
}

func (s *SGServer) process(ctx context.Context, request *protocol.Message, response *protocol.Message) *protocol.Message {
	if request.MessageType == protocol.MessageTypeHeartbeat {
		response.MessageType = protocol.MessageTypeHeartbeat
		return response
	}

	sname := request.ServiceName
	mname := request.MethodName
	srvInterface, ok := s.serviceMap.Load(sname)
	if !ok {
		return errorResponse(response, "can not find service")
	}

	srv, ok := srvInterface.(*service)
	if !ok {
		return errorResponse(response, "not *service type")
	}

	mtypeInterface, ok := srv.methods.Load(mname)
	mtype, ok := mtypeInterface.(*methodType)
	if !ok {
		return errorResponse(response, "can not find method")
	}

	argv := newValue(mtype.ArgType)
	replyv := newValue(mtype.ReplyType)

	actualCodec := s.codec
	if request.SerializeType != s.Option.SerializeType {
		actualCodec = codec.GetCodec(request.SerializeType)
	}
	err := actualCodec.Decode(request.Data, argv)
	if err != nil {
		return errorResponse(response, "decode arg error: "+err.Error())
	}

	var returns []reflect.Value
	if mtype.ArgType.Kind() != reflect.Ptr {
		returns = mtype.method.Func.Call([]reflect.Value{
			srv.rcvr,
			reflect.ValueOf(ctx),
			reflect.ValueOf(argv).Elem(),
			reflect.ValueOf(replyv),
		})
	} else {
		returns = mtype.method.Func.Call([]reflect.Value{
			srv.rcvr,
			reflect.ValueOf(ctx),
			reflect.ValueOf(argv),
			reflect.ValueOf(replyv),
		})
	}

	if len(returns) > 0 && returns[0].Interface() != nil {
		err = returns[0].Interface().(error)
		return errorResponse(response, err.Error())
	}

	responseData, err := actualCodec.Encode(replyv)
	if err != nil {
		return errorResponse(response, err.Error())
	}

	response.StatusCode = protocol.StatusOK
	response.Data = responseData

	return response
}

func errorResponse(message *protocol.Message, err string) *protocol.Message {
	message.Error = err
	message.StatusCode = protocol.StatusError
	message.Data = message.Data[:0]
	return message
}

func newValue(t reflect.Type) interface{} {
	if t.Kind() == reflect.Ptr {
		return reflect.New(t.Elem()).Interface()
	} else {
		return reflect.New(t).Interface()
	}
}

func (s *SGServer) writeErrorResponse(response *protocol.Message, w io.Writer, err string) {
	response.Error = err
	log.Printf("response error: %v", response.Error)
	response.StatusCode = protocol.StatusError
	response.Data = response.Data[:0] // 将数据置空
	_, _ = w.Write(protocol.EncodeMessage(s.Option.ProtocolType, response))
}

func (s *SGServer) wrapHandleRequest(handleFunc HandleRequestFunc) HandleRequestFunc {
	for _, w := range s.Option.Wrappers {
		handleFunc = w.WrapHandleRequest(s, handleFunc)
	}
	return handleFunc
}

func (s *SGServer) Service() []ServiceInfo {
	var srvs []ServiceInfo
	s.serviceMap.Range(func(key, value interface{}) bool {
		sname, ok := key.(string)
		if ok {
			srv, ok := value.(*service)
			if ok {
				var methodList []string
				srv.methods.Range(func(key, value interface{}) bool {
					if m, ok := value.(*methodType); ok {
						methodList = append(methodList, m.method.Name)
					}
					return true
				})
				srvs = append(srvs, ServiceInfo{sname, methodList})
			}
		}
		return true
	})
	return srvs
}

// Close 优雅退出
func (s *SGServer) Close() error {
	closeFunc := s.close

	for _, w := range s.Option.Wrappers {
		closeFunc = w.WrapClose(s, closeFunc)
	}

	return closeFunc()
}

func (s *SGServer) close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.shutdown = true

	// 等待当前请求处理完成或者直到指定的时间
	ticker := time.NewTicker(s.Option.ShutDownWait)
	defer ticker.Stop()
	// 这里会先判断当前是否还有为处理完成的请求，有的话等待一段时间后再退出
	for {
		// requestInProcess 表示当前正在处理的请求数，在 wrapper 里计数
		if s.RequestInProcess <= 0 {
			break
		}
		select {
		case <-ticker.C:
			break
		}
	}

	if s.tr != nil {
		return s.tr.Close()
	}

	return nil
}

func (s *SGServer) AddShutdownHook(hook ShutDownHook) {
	s.mutex.Lock()
	s.Option.ShutDownHooks = append(s.Option.ShutDownHooks, hook)
	s.mutex.Unlock()
}

func (s *SGServer) ServeTransport(tr transport.Transport) {
	serveFunc := s.serverTransport
	s.wrapServeTransport(serveFunc)(tr)
}

func (s *SGServer) wrapServeTransport(transportFunc ServeTransportFunc) ServeTransportFunc {
	for _, w := range s.Option.Wrappers {
		transportFunc = w.WrapServeTransport(s, transportFunc)
	}
	return transportFunc
}

func (s *SGServer) writeResponse(ctx context.Context, tr transport.Transport, response *protocol.Message) {
	deadline, ok := ctx.Deadline()
	if ok {
		if time.Now().Before(deadline) {
			_, err := tr.Write(protocol.EncodeMessage(s.Option.ProtocolType, response))
			if err != nil {
				log.Println("write response error:" + err.Error())
			}
		} else {
			log.Println("passed deadline, give up write response")
		}
	} else {
		_, err := tr.Write(protocol.EncodeMessage(s.Option.ProtocolType, response))
		if err != nil {
			log.Println("write response error:" + err.Error())
		}
	}
}
