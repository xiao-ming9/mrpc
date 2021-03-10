package server

import (
	"context"
	"errors"
	"io"
	"log"
	"mrpc/codec"
	"mrpc/protocol"
	"mrpc/transport"
	"reflect"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"
)

type RpcServer interface {
	// 注册服务实例，receiver 是对外暴露的方法的实现者
	// metaData 是注册服务时携带的额外元数据，描述了 receiver 的其他信息
	Register(receiver interface{}, metaData map[string]string) error

	// 开始对外提供服务的接口
	Serve(network string, addr string) error
	// Service() []ServiceInfo
	Close() error
}

// simpleServer 实现了 RPCServer 接口
type simpleServer struct {
	codec      codec.Codec
	serviceMap sync.Map
	tr         transport.ServerTransport
	mutex      sync.Mutex
	shutdown   bool

	option Option
}

type ServiceInfo struct {
	Name    string   `json:"name"`
	Methods []string `json:"methods"`
}


type service struct {
	name    string
	typ     reflect.Type
	rcvr    reflect.Value
	methods map[string]*methodType
}

func (s *simpleServer) Register(receiver interface{}, metaData map[string]string) error {
	recvTyp := reflect.TypeOf(receiver)
	name := recvTyp.Name()
	srv := new(service)
	srv.name = name
	srv.rcvr = reflect.ValueOf(receiver)
	srv.typ = recvTyp
	methods := suitableMethods(recvTyp, true)
	srv.methods = methods

	if len(srv.methods) == 0 {
		var errorStr string

		// 如果对应的类型没有任何符合规则的方法，扫描对应的指针（参考 net.rpc 包）
		method := suitableMethods(reflect.PtrTo(srv.typ), false)
		if len(method) != 0 {
			errorStr = "mrpc.Register: type " + name + " has no exported methods of suitable type (hint: pass a pointer to value of that type)"
		} else {
			errorStr = "mrpc.Register: type " + name + " has no exported methods of suitable type"
		}
		log.Println(errorStr)

		return errors.New(errorStr)
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
		methods[mname] = &methodType{
			method:    method,
			ArgType:   argType,
			ReplyType: replyType,
		}
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
	rune, _ := utf8.DecodeRuneInString(name)
	return unicode.IsUpper(rune)
}

func (s *simpleServer) Serve(network string, addr string) error {
	s.tr = transport.NewServerTransport(s.option.TransportType)
	err := s.tr.Listen(network, addr)
	if err != nil {
		log.Println(err)
		return err
	}
	for {
		conn, err := s.tr.Accept()
		if err != nil {
			log.Println(err)
			return err
		}
		go s.serverTransport(conn)
	}
}

// serverTransport 在一个无限循环中从 Transport 读取数据并反序列化成请求，根据请求指定的方法查找自身缓存的方法，
// 找到对应的方法后通过反射执行对应的实现并返回。执行完成后再根据返回结果或者是执行发生的异常组装成一个完整的消息，通过Transport发送出去。
func (s *simpleServer) serverTransport(tr transport.Transport) {
	for {
		request, err := protocol.DecodeMessage(s.option.ProtocolType, tr)

		if err != nil {
			if err == io.EOF {
				log.Printf("client has closed this connection: %s", tr.RemoteAddr().String())
			} else if strings.Contains(err.Error(), " use of closed network connection") {
				log.Printf("mrpc: connection %s is closed", tr.RemoteAddr().String())
			} else {
				log.Printf("mrpc: failed to read request: %v", err)
				panic(err)
			}
			return
		}
		response := request.Clone()
		response.MessageType = protocol.MessageTypeResponse

		sname := request.ServiceName
		mname := request.MethodName
		srvInterface, ok := s.serviceMap.Load(sname)
		if !ok {
			s.writeErrorResponse(response, tr, err.Error())
			return
		}

		srv, ok := srvInterface.(*service)
		if !ok {
			s.writeErrorResponse(response, tr, "not *service type")
			return
		}

		mtype, ok := srv.methods[mname]
		if !ok {
			s.writeErrorResponse(response, tr, "can not find method")
			return
		}

		argv := newValue(mtype.ArgType)
		replyv := newValue(mtype.ReplyType)

		ctx := context.Background()
		err = s.codec.Decode(request.Data, argv)

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
			s.writeErrorResponse(response, tr, err.Error())
			return
		}

		responseData, err := codec.GetCodec(request.SerializeType).Encode(replyv)
		if err != nil {
			s.writeErrorResponse(response, tr, err.Error())
			return
		}

		response.StatusCode = protocol.StatusOk
		response.Data = responseData

		_, err = tr.Write(protocol.EncodeMessage(s.option.ProtocolType, response))
		if err != nil {
			log.Println(err)
			return
		}
	}
}

func (s *simpleServer) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.shutdown = true

	err := s.tr.Close()

	s.serviceMap.Range(func(key, value interface{}) bool {
		s.serviceMap.Delete(key)
		return true
	})

	return err
}

func newValue(t reflect.Type) interface{} {
	if t.Kind() == reflect.Ptr {
		return reflect.New(t.Elem()).Interface()
	} else {
		return reflect.New(t).Interface()
	}
}

func (s *simpleServer) writeErrorResponse(response *protocol.Message, w io.Writer, err string) {
	response.Error = err
	log.Println(response.Error)
	response.StatusCode = protocol.StatusError
	response.Data = response.Data[:0] // 将数据置空
	_, _ = w.Write(protocol.EncodeMessage(s.option.ProtocolType, response))
}

type methodType struct {
	method    reflect.Method
	ArgType   reflect.Type
	ReplyType reflect.Type
}

func NewSimpleServer(option Option) RpcServer {
	s := new(simpleServer)
	s.option = option
	s.codec = codec.GetCodec(option.SerializeType)
	return s
}
