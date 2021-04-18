package protocol

import (
	"encoding/binary"
	"errors"
	"github.com/vmihailenco/msgpack"
	"io"
	"mrpc/codec"
	"time"
)

// 自定义消息协议
// -------------------------------------------------------------------------------------------------
// |2byte|1byte  |4byte       |4byte        | header length |(total length - header length - 4byte)|
// -------------------------------------------------------------------------------------------------
// |magic|version|total length|header length|     header    |                    body              |
// -------------------------------------------------------------------------------------------------

var MAGIC = []byte{0xab, 0xba}

const (
	RequestSeqKey      = "rpc_request_seq"
	RequestTimeoutKey  = "rpc_request_timeout"
	RequestDeadlineKey = "rpc_request_deadline"
	MetaDataKey        = "rpc_meta_data"
	ProviderDegradeKey = "provider_degrade"
	AuthKey            = "rpc_auth"
)

type MessageType byte

// 请求类型
const (
	MessageTypeRequest MessageType = iota
	MessageTypeResponse
	MessageTypeHeartbeat
)

func ParseMessageType(name string) (MessageType, error) {
	switch name {
	case "request":
		return MessageTypeRequest, nil
	case "response":
		return MessageTypeResponse, nil
	case "heartbeat":
		return MessageTypeHeartbeat, nil
	default:
		return MessageTypeRequest, errors.New("type " + name + " not found")
	}
}

func (messageType MessageType) String() string {
	switch messageType {
	case MessageTypeRequest:
		return "request"
	case MessageTypeResponse:
		return "response"
	case MessageTypeHeartbeat:
		return "heartbeat"
	default:
		return "unknown"
	}
}

// CompressType 用来标识一个消息的压缩方式
type CompressType byte

const (
	CompressTypeNone CompressType = iota
)

func ParseCompressType(name string) (CompressType, error) {
	switch name {
	case "none":
		return CompressTypeNone, nil
	default:
		return CompressTypeNone, errors.New("type " + name + " not found")
	}
}

func (compressType CompressType) String() string {
	switch compressType {
	case CompressTypeNone:
		return "none"
	default:
		return "unknown"
	}
}

type StatusCode byte

const (
	StatusOK StatusCode = iota
	StatusError
)

func ParseStatusCode(name string) (StatusCode, error) {
	switch name {
	case "ok":
		return StatusOK, nil
	case "error":
		return StatusError, nil
	default:
		return StatusError, errors.New("type " + name + " not found")
	}
}

func (code StatusCode) String() string {
	switch code {
	case StatusOK:
		return "ok"
	case StatusError:
		return "error"
	default:
		return "unknown"
	}
}

type ProtocolType byte

const (
	Default ProtocolType = iota
)

var protocols = map[ProtocolType]Protocol{
	Default: &RPCProtocol{},
}

// Protocol 定义了如何构造和序列化一个完整的消息体
type Protocol interface {
	NewMessage() *Message
	DecodeMessage(r io.Reader) (*Message, error)
	EncodeMessage(message *Message) []byte
}

type Message struct {
	*Header        // head部分
	Data    []byte // body部分
}

func (m Message) Clone() *Message {
	header := *m.Header
	c := new(Message)
	c.Header = &header
	c.Data = m.Data
	return c
}

func (m *Message) Deadline() (time.Time, bool) {
	if m.MetaData == nil {
		return time.Now(), false
	} else {
		deadline, ok := m.MetaData[RequestDeadlineKey]
		if ok {
			switch deadline.(type) {
			case time.Time:
				return deadline.(time.Time), ok
			case *time.Time:
				return *deadline.(*time.Time), ok
			default:
				return time.Now(), false
			}
		} else {
			return time.Now(), false
		}
	}
}

// Header 消息头信息
type Header struct {
	Seq           uint64                 // 序号，用来唯一标识请求或响应
	MessageType   MessageType            // 消息类型，用来标识一个消息是请求还是响应
	CompressType  CompressType           // 压缩类型，用来标识一个消息的压缩方式
	SerializeType codec.SerializeType    // 序列化类型，用来标识消息体采用的编码方式
	StatusCode    StatusCode             // 状态类型，用来标识一个请求是正常还是异常
	ServiceName   string                 // 服务名
	MethodName    string                 // 方法名
	Error         string                 // 方法调用发生异常
	MetaData      map[string]interface{} // 其他元数据
}

type RPCProtocol struct {
}

func (R RPCProtocol) NewMessage() *Message {
	return &Message{Header: &Header{}}
}

func (R RPCProtocol) DecodeMessage(r io.Reader) (msg *Message, err error) {
	first3bytes := make([]byte, 3)
	_, err = io.ReadFull(r, first3bytes)
	if err != nil {
		return
	}
	if !CheckMagic(first3bytes[:2]) {
		err = errors.New("wrong protocol")
		return
	}
	totalLenBytes := make([]byte, 4)
	_, err = io.ReadFull(r, totalLenBytes)
	if err != nil {
		return
	}
	// Todo: 该偏向于简单而不是效率。 需要高性能序列化的客户，特别是对于大型数据结构的客户，应该查看更高级的解决方案，例如编码/目标包或协议缓冲区
	totalLen := int(binary.BigEndian.Uint32(totalLenBytes))
	if totalLen < 4 {
		err = errors.New("invalid total length")
		return
	}
	data := make([]byte, totalLen)
	_, err = io.ReadFull(r, data)
	headerLen := int(binary.BigEndian.Uint32(data[:4]))
	headerBytes := data[4 : headerLen+4]
	header := &Header{}
	err = msgpack.Unmarshal(headerBytes, header)
	if err != nil {
		return
	}
	msg = new(Message)
	msg.Header = header
	msg.Data = data[headerLen+4:]
	return
}

func (R RPCProtocol) EncodeMessage(msg *Message) []byte {
	// 第三个是版本号，暂时不用，保留
	first3bytes := []byte{0xab, 0xba, 0x00}
	headerBytes, _ := msgpack.Marshal(msg.Header)

	totalLen := 4 + len(headerBytes) + len(msg.Data)
	totalLenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(totalLenBytes, uint32(totalLen))

	// 整合消息协议到 data 中
	data := make([]byte, totalLen+7)
	start := 0
	copyFullWithOffset(data, first3bytes, &start)
	copyFullWithOffset(data, totalLenBytes, &start)

	headerLenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(headerLenBytes, uint32(len(headerBytes)))
	copyFullWithOffset(data, headerLenBytes, &start)
	copyFullWithOffset(data, headerBytes, &start)
	copyFullWithOffset(data, msg.Data, &start)
	return data
}

func NewMessage(t ProtocolType) *Message {
	return protocols[t].NewMessage()
}

func DecodeMessage(t ProtocolType, r io.Reader) (*Message, error) {
	return protocols[t].DecodeMessage(r)
}

func EncodeMessage(t ProtocolType, m *Message) []byte {
	return protocols[t].EncodeMessage(m)
}

// CheckMagic 鉴别是否是非法请求
func CheckMagic(bytes []byte) bool {
	return bytes[0] == MAGIC[0] && bytes[1] == MAGIC[1]
}

func copyFullWithOffset(dst []byte, src []byte, start *int) {
	copy(dst[*start:*start+len(src)], src)
	*start = *start + len(src)
}
