package transport

import (
	"io"
	"net"
)

type transportMaker func() Transport

type serverTransportMaker func() ServerTransport

type TransportType byte

const TCPTransport TransportType = iota

var makeTransport = map[TransportType]transportMaker{
	TCPTransport: func() Transport {
		return new(Socket)
	},
}

var makeServerTransport = map[TransportType]serverTransportMaker{
	TCPTransport: func() ServerTransport {
		return new(ServerSocket)
	},
}

// 传输层的定义，用于读取数据
type Transport interface {
	Dial(network, addr string) error
	//这里直接内嵌了 ReadWriteCloser 接口，包含 Read、Write 和 Close 方法
	io.ReadWriteCloser
	RemoteAddr() net.Addr
	LocalAddr() net.Addr
}

type Socket struct {
	conn net.Conn
}

func NewTransport(t TransportType) Transport {
	return makeTransport[t]()
}

func (s *Socket) Dial(network, addr string) error {
	conn, err := net.Dial(network, addr)
	s.conn = conn
	return err
}

func (s *Socket) Read(p []byte) (n int, err error) {
	return s.conn.Read(p)
}

func (s *Socket) Write(p []byte) (n int, err error) {
	return s.conn.Write(p)
}

func (s *Socket) Close() error {
	return s.conn.Close()
}

func (s *Socket) RemoteAddr() net.Addr {
	return s.conn.RemoteAddr()
}

func (s *Socket) LocalAddr() net.Addr {
	return s.conn.LocalAddr()
}

// 服务端监听器定义，用于监听端口和建立连接
type ServerTransport interface {
	// Listen 监听端口
	Listen(network, addr string) error
	// Accept 建立连接
	Accept() (Transport, error)
	// 这里直接内嵌了 Closer 接口，包含 Close 方法
	io.Closer
}

type ServerSocket struct {
	ln net.Listener
}

func NewServerTransport(t TransportType) ServerTransport {
	return makeServerTransport[t]()
}

func (s *ServerSocket) Listen(network, addr string) error {
	ln, err := net.Listen(network, addr)
	s.ln = ln
	return err
}

func (s *ServerSocket) Accept() (Transport, error) {
	conn, err := s.ln.Accept()
	return &Socket{conn: conn}, err
}

func (s *ServerSocket) Close() error {
	return s.ln.Close()
}
