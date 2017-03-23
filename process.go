package thrifttools

import (
	"reflect"

	"sync"

	"git.apache.org/thrift.git/lib/go/thrift"
)

type HandlerFunc func(c *Context)
type handlersChain []HandlerFunc

type ThriftProcessor interface {
	GetProcessorFunction(key string) (processor thrift.TProcessorFunction, ok bool)
	Process(iprot, oprot thrift.TProtocol) (success bool, err thrift.TException)
}

type ThriftMidWare struct {
	thriftHandler  interface{}
	handlers       []HandlerFunc
	methodHandlers map[string][]HandlerFunc
	pool           sync.Pool
	methods        map[string]reflect.Value
	insType        map[string][]reflect.Type
	outsType       map[string][]reflect.Type
}

func NewThriftMidWare(thriftHandler interface{}) *ThriftMidWare {
	middleware := &ThriftMidWare{
		thriftHandler: thriftHandler,
	}
	middleware.pool.New = func() interface{} {
		return middleware.allocateContext()
	}

	handlerValue := reflect.ValueOf(thriftHandler)
	handlerType := reflect.TypeOf(thriftHandler)
	numMethod := handlerType.NumMethod()
	middleware.methods = make(map[string]reflect.Value, numMethod)
	middleware.insType = make(map[string][]reflect.Type, numMethod)
	middleware.outsType = make(map[string][]reflect.Type, numMethod)

	for m := 0; m < numMethod; m++ {
		methodValue := handlerValue.Method(m)
		methodType := methodValue.Type()
		method := handlerType.Method(m)
		middleware.methods[method.Name] = methodValue
		numIn := methodType.NumIn()
		ins := make([]reflect.Type, 0, numIn)
		for i := 0; i < numIn; i++ {
			ins = append(ins, methodType.In(i))
		}

		middleware.insType[method.Name] = ins
		numOut := methodType.NumOut()
		outs := make([]reflect.Type, 0, numOut)
		for o := 0; o < numOut; o++ {
			outs = append(outs, methodType.Out(0))
		}
		middleware.outsType[method.Name] = outs
	}

	return middleware
}

func (this *ThriftMidWare) allocateContext() *Context {
	return &Context{Midware: this}
}

func (this *ThriftMidWare) Process(iprot, oprot thrift.TProtocol) (success bool, err thrift.TException) {
	name, _, seqId, err := iprot.ReadMessageBegin()
	if err != nil {
		return false, err
	}

	c := this.pool.Get().(*Context)
	c.reset()
	c.SeqId = seqId
	c.Iprot = iprot
	c.Oprot = oprot

	c.Name = name
	c.Midware = this
	c.Method = this.methods[c.Name]
	if !c.Method.IsValid() {
		c.Iprot.Skip(thrift.STRUCT)
		c.Iprot.ReadMessageEnd()
		x73 := thrift.NewTApplicationException(thrift.UNKNOWN_METHOD, "Unknown function "+c.Name)
		c.Oprot.WriteMessageBegin(c.Name, thrift.EXCEPTION, c.SeqId)
		x73.Write(c.Oprot)
		c.Oprot.WriteMessageEnd()
		c.Oprot.Flush()
		return false, x73
	}

	c.Ins, err = unpack(this.insType[c.Name], c.Iprot)
	if err != nil {
		c.Iprot.ReadMessageEnd()
		x := thrift.NewTApplicationException(thrift.PROTOCOL_ERROR, err.Error())
		c.Oprot.WriteMessageBegin(c.Name, thrift.EXCEPTION, c.SeqId)
		x.Write(c.Oprot)
		c.Oprot.WriteMessageEnd()
		c.Oprot.Flush()
		return false, err
	}
	iprot.ReadMessageEnd()

	defer this.pool.Put(c)
	return this.doProcess(c)

}
func (this *ThriftMidWare) doProcess(c *Context) (success bool, err thrift.TException) {

	c.handlers = this.handlers
	if hls, ok := this.methodHandlers[c.Name]; ok {
		c.handlers = append(c.handlers, hls...)
	}
	fn := func(c *Context) {
		c.Outs = c.Method.Call(c.Ins)
	}

	c.handlers = append(c.handlers, fn)
	c.Next()

	for _, out := range c.Outs {
		if !IsNull(out) {
			switch out.Interface().(type) {
			case ThriftException:
			case error:
				err := out.Interface().(error)
				x := thrift.NewTApplicationException(thrift.INTERNAL_ERROR, "Internal error processing Set: "+err.Error())
				c.Oprot.WriteMessageBegin(c.Name, thrift.EXCEPTION, c.SeqId)
				x.Write(c.Oprot)
				c.Oprot.WriteMessageEnd()
				c.Oprot.Flush()
				return true, err
			}
		}
	}

	if c.IsAbort() && c.Err != nil {
		x := thrift.NewTApplicationException(thrift.INTERNAL_ERROR, "user middleware  error processing Set: "+c.Err.Error())
		c.Oprot.WriteMessageBegin(c.Name, thrift.EXCEPTION, c.SeqId)
		x.Write(c.Oprot)
		c.Oprot.WriteMessageEnd()
		c.Oprot.Flush()
		return true, x
	}
	var err2 error
	restuts_name := c.Name + "_result"
	if err2 = c.Oprot.WriteMessageBegin(c.Name, thrift.REPLY, c.SeqId); err2 != nil {
		err = err2
	}
	if err2 = pack(restuts_name, c.Outs, c.Oprot); err == nil && err2 != nil {
		err = err2
	}
	if err2 = c.Oprot.WriteMessageEnd(); err == nil && err2 != nil {
		err = err2
	}
	if err2 = c.Oprot.Flush(); err == nil && err2 != nil {
		err = err2
	}
	if err != nil {
		return
	}
	return true, err
}

func (this *ThriftMidWare) Use(handles ...HandlerFunc) {
	this.handlers = append(this.handlers, handles...)
}

func (this *ThriftMidWare) MethodUse(method string, handles ...HandlerFunc) {

	if _, ok := this.methodHandlers[method]; ok {
		this.methodHandlers[method] = append(this.methodHandlers[method], handles...)
	} else {
		handlers := handlersChain(handles)
		this.methodHandlers[method] = handlers
	}
}
