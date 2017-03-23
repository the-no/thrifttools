package thrifttools

import (
	"math"
	"reflect"

	"git.apache.org/thrift.git/lib/go/thrift"
)

const abortIndex int8 = math.MaxInt8 / 2

type Context struct {
	SeqId int32
	Iprot thrift.TProtocol
	Oprot thrift.TProtocol
	Err   error

	Name     string
	handlers handlersChain
	index    int8
	Midware  *ThriftMidWare
	Ins      []reflect.Value
	Outs     []reflect.Value
	Method   reflect.Value
}

func (c *Context) reset() {
	c.SeqId = -1
	c.Iprot = nil
	c.Oprot = nil

	c.Err = nil

	c.Name = ""
	c.handlers = c.handlers[0:0]
	c.index = -1
	c.Midware = nil
}
func (c *Context) Next() {
	c.index++
	s := int8(len(c.handlers))
	for ; c.index < s; c.index++ {
		c.handlers[c.index](c)
	}
}
func (c *Context) AbortWithError(err error) {
	c.Err = err
	c.index = abortIndex
}

func (c *Context) IsAbort() bool {
	return c.index == abortIndex
}
