package thrifttools

import (
	"git.apache.org/thrift.git/lib/go/thrift"
)

type ProtocolHandler func(p *Protocol)
type Protocol struct {
	thrift.TProtocol
	WriteBeginAction func(name string, typeId thrift.TMessageType, seqId int32)
	WriteEndAction   ProtocolHandler
	ReadBeginAction  func(name string, typeId thrift.TMessageType, seqId int32, err error)
	ReadEndAction    ProtocolHandler
}

type TBinaryProtocolFactory struct {
	thrift.TBinaryProtocolFactory

	PWriteBeginAction func(name string, typeId thrift.TMessageType, seqId int32)
	PWriteEndAction   ProtocolHandler
	PReadBeginAction  func(name string, typeId thrift.TMessageType, seqId int32, err error)
	PReadEndAction    ProtocolHandler
}

/*func NewProtocolTransport(t thrift.TTransport) *Protocol {
	return NewProtocol(t, false, true)
}*/

/*
func NewProtocol(t thrift.TTransport, strictRead, strictWrite bool) *Protocol {
	//p := &Protocol{thrift.TBinaryProtocol{origTransport: t, strictRead: strictRead, strictWrite: strictWrite}}
	p := &Protocol{*thrift.NewTBinaryProtocol(t, strictRead, strictWrite), nil, nil, nil, nil}
	if et, ok := t.(thrift.TRichTransport); ok {
		p.trans = et
	} else {
		p.trans = thrift.NewTRichTransport(t)
	}
	p.reader = p.trans
	p.writer = p.trans
	return p
}
*/
func NewTBinaryProtocolFactoryDefault() *TBinaryProtocolFactory {
	return NewTBinaryProtocolFactory(false, true)
}

func NewTBinaryProtocolFactory(strictRead, strictWrite bool) *TBinaryProtocolFactory {
	//return &ProtocolFactory{thrift.TBinaryProtocolFactory{strictRead: strictRead, strictWrite: strictWrite}}
	return &TBinaryProtocolFactory{*thrift.NewTBinaryProtocolFactory(strictRead, strictWrite), nil, nil, nil, nil}
}

func (p *TBinaryProtocolFactory) GetProtocol(t thrift.TTransport) thrift.TProtocol {
	return &Protocol{
		p.TBinaryProtocolFactory.GetProtocol(t),
		p.PWriteBeginAction,
		p.PWriteEndAction,
		p.PReadBeginAction,
		p.PReadEndAction,
	}
	//proto := p.TBinaryProtocolFactory.GetProtocol(t).(*thrift.TBinaryProtocol)
	//return &Protocol{proto, nil, nil, nil, nil}
}

func (p *Protocol) WriteMessageBegin(name string, typeId thrift.TMessageType, seqId int32) error {
	if p.WriteBeginAction != nil {
		p.WriteBeginAction(name, typeId, seqId)
	}
	return p.TProtocol.WriteMessageBegin(name, typeId, seqId)

}

func (p *Protocol) WriteMessageEnd() (err error) {

	err = p.TProtocol.WriteMessageEnd()

	if p.WriteEndAction != nil {
		p.WriteEndAction(p)
	}
	return
}

func (p *Protocol) ReadMessageBegin() (name string, typeId thrift.TMessageType, seqId int32, err error) {
	if p.ReadBeginAction != nil {
		p.ReadBeginAction(name, typeId, seqId, err)
	}
	return p.TProtocol.ReadMessageBegin()
}

func (p *Protocol) ReadMessageEnd() (err error) {
	err = p.TProtocol.ReadMessageEnd()
	if p.ReadEndAction != nil {
		p.ReadEndAction(p)
	}
	return
}

//WriteMessageBegin(name string, typeId TMessageType, seqid int32) error
//WriteMessageEnd() error
/*func (p *Protocol) WriteStructBegin(name string) error {
	return p.TProtocol.WriteStructBegin(name)
}
func (p *Protocol) WriteStructEnd() error {
	return p.TProtocol.WriteStructEnd()
}
func (p *Protocol) WriteFieldBegin(name string, typeId thrift.TType, id int16) error {
	return p.TProtocol.WriteFieldBegin(name, typeId, id)
}
func (p *Protocol) WriteFieldEnd() error {
	return p.TProtocol.WriteFieldEnd()
}
func (p *Protocol) WriteFieldStop() error {
	return p.TProtocol.WriteFieldStop()
}
func (p *Protocol) WriteMapBegin(keyType thrift.TType, valueType thrift.TType, size int) error {
	return p.TProtocol.WriteMapBegin(keyType, valueType, size)
}

func (p *Protocol) WriteMapEnd() error {
	return p.TProtocol.WriteMapEnd()
}
func (p *Protocol) WriteListBegin(elemType thrift.TType, size int) error {
	return p.TProtocol.WriteListBegin(elemType, size)
}
func (p *Protocol) WriteListEnd() error {
	return p.TProtocol.WriteListEnd()
}
func (p *Protocol) WriteSetBegin(elemType thrift.TType, size int) error {
	return p.TProtocol.WriteSetBegin(elemType, size)
}
func (p *Protocol) WriteSetEnd() error {
	return p.TProtocol.WriteSetEnd()
}
func (p *Protocol) WriteBool(value bool) error {
	return p.TProtocol.WriteBool(value)
}
func (p *Protocol) WriteByte(value int8) error {
	return p.TProtocol.WriteByte(value)
}
func (p *Protocol) WriteI16(value int16) error {
	return p.TProtocol.WriteI16(value)
}
func (p *Protocol) WriteI32(value int32) error {
	return p.TProtocol.WriteI32(value)
}
func (p *Protocol) WriteI64(value int64) error {
	return p.TProtocol.WriteI64(value)
}
func (p *Protocol) WriteDouble(value float64) error {
	return p.TProtocol.WriteDouble(value)
}
func (p *Protocol) WriteString(value string) error {
	return p.TProtocol.WriteString(value)
}
func (p *Protocol) WriteBinary(value []byte) error {
	return p.TProtocol.WriteBinary(value)
}*/

//ReadMessageBegin() (name string, typeId TMessageType, seqid int32, err error)
//	ReadMessageEnd() error
/*func (p *Protocol) ReadStructBegin() (name string, err error) {
	return p.TProtocol.ReadStructBegin()
}
func (p *Protocol) ReadStructEnd() error {
	return p.TProtocol.ReadStructEnd()
}
func (p *Protocol) ReadFieldBegin() (name string, typeId thrift.TType, id int16, err error) {
	return p.TProtocol.ReadFieldBegin()
}
func (p *Protocol) ReadFieldEnd() error {
	return p.TProtocol.ReadFieldEnd()
}
func (p *Protocol) ReadMapBegin() (keyType thrift.TType, valueType thrift.TType, size int, err error) {
	return p.TProtocol.ReadMapBegin()
}
func (p *Protocol) ReadMapEnd() error {
	return p.TProtocol.ReadMapEnd()
}
func (p *Protocol) ReadListBegin() (elemType thrift.TType, size int, err error) {
	return p.TProtocol.ReadListBegin()
}
func (p *Protocol) ReadListEnd() error {
	return p.TProtocol.ReadListEnd()
}
func (p *Protocol) ReadSetBegin() (elemType thrift.TType, size int, err error) {
	return p.TProtocol.ReadSetBegin()
}
func (p *Protocol) ReadSetEnd() error {
	return p.TProtocol.ReadSetEnd()
}
func (p *Protocol) ReadBool() (value bool, err error) {
	return p.TProtocol.ReadBool()
}
func (p *Protocol) ReadByte() (value int8, err error) {
	return p.TProtocol.ReadByte()
}
func (p *Protocol) ReadI16() (value int16, err error) {
	return p.TProtocol.ReadI16()
}
func (p *Protocol) ReadI32() (value int32, err error) {
	return p.TProtocol.ReadI32()
}
func (p *Protocol) ReadI64() (value int64, err error) {
	return p.TProtocol.ReadI64()
}
func (p *Protocol) ReadDouble() (value float64, err error) {
	return p.TProtocol.ReadDouble()
}
func (p *Protocol) ReadString() (value string, err error) {
	return p.TProtocol.ReadString()
}
func (p *Protocol) ReadBinary() (value []byte, err error) {
	return p.TProtocol.ReadBinary()
}

func (p *Protocol) Skip(fieldType thrift.TType) (err error) {
	return p.TProtocol.Skip(fieldType)
}
func (p *Protocol) Flush() (err error) {
	return p.TProtocol.Flush()
}

func (p *Protocol) Transport() thrift.TTransport {
	return p.TProtocol.Transport()
}
*/
