package thrifttools

import (
	"fmt"
	"reflect"

	"git.apache.org/thrift.git/lib/go/thrift"
)

type ThriftReader interface {
	Read(iprot thrift.TProtocol) error
}
type ThriftWriter interface {
	Write(oprot thrift.TProtocol) error
}
type ThriftStruct interface {
	ThriftReader
	ThriftWriter
	String() string
}
type ThriftException interface {
	ThriftStruct
	Error() string
}

var Typmap = map[reflect.Kind]thrift.TType{
	reflect.Invalid: thrift.STOP,
	reflect.Bool:    thrift.BOOL,
	reflect.Int8:    thrift.BYTE,
	reflect.Int16:   thrift.I16,
	reflect.Int32:   thrift.I32,

	reflect.Int64:   thrift.I64,
	reflect.Float64: thrift.DOUBLE,
	reflect.String:  thrift.STRING,
	reflect.Map:     thrift.MAP,
	reflect.Slice:   thrift.LIST,

	reflect.Struct:    thrift.STRUCT,
	reflect.Ptr:       thrift.STRUCT,
	reflect.Interface: thrift.STRUCT,
}

func unpack(instype []reflect.Type, iprot thrift.TProtocol) (args []reflect.Value, err error) {

	if _, err = iprot.ReadStructBegin(); err != nil {
		err = thrift.PrependError(fmt.Sprintf("read  args error: "), err)
		return
	}
	for i := 0; ; i++ {
		_, fieldTypeId, fieldId, err2 := iprot.ReadFieldBegin()
		if err2 != nil {
			err = thrift.PrependError(fmt.Sprintf("field %d read error: ", fieldId), err2)
			return
		}
		if fieldTypeId == thrift.STOP {
			break
		}

		v, err2 := readArg(instype[i], iprot)
		if err2 != nil {
			err = err2
			return
		}

		if err = iprot.ReadFieldEnd(); err != nil {
			return
		}
		args = append(args, v)
	}

	if err = iprot.ReadStructEnd(); err != nil {
		err = thrift.PrependError(fmt.Sprintf("read struct end error: "), err)
		return
	}
	return args, nil
}

func readArg(tpy reflect.Type, iprot thrift.TProtocol) (value reflect.Value, err error) {
	var err2 error
	switch tpy.Kind() {
	case reflect.Bool:
		var in bool
		if in, err = iprot.ReadBool(); err != nil {
			err2 = thrift.PrependError("error reading field: ", err)
		} else {
			value = reflect.ValueOf(in)
		}
	case reflect.Int8:
		var in int8
		if in, err = iprot.ReadByte(); err != nil {
			err2 = thrift.PrependError("error reading field: ", err)
		} else {
			value = reflect.ValueOf(in)
		}
	case reflect.Int16:
		var in int16
		if in, err = iprot.ReadI16(); err != nil {
			err2 = thrift.PrependError("error reading field: ", err)
		} else {
			value = reflect.ValueOf(in)
		}
	case reflect.Int32:
		var in int32
		if in, err = iprot.ReadI32(); err != nil {
			err2 = thrift.PrependError("error reading field: ", err)
		} else {
			value = reflect.ValueOf(in)
		}
	case reflect.Int64:
		var in int64
		if in, err = iprot.ReadI64(); err != nil {
			err2 = thrift.PrependError("error reading field: ", err)
		} else {
			value = reflect.ValueOf(in)
		}

	case reflect.Float64:
		var in float64
		if in, err = iprot.ReadDouble(); err != nil {
			err2 = thrift.PrependError("error reading field: ", err)
		} else {
			value = reflect.ValueOf(in)
		}
	case reflect.Map:
		_, _, size, err := iprot.ReadMapBegin()
		if err != nil {
			err2 = thrift.PrependError("error reading map begin: ", err)
			break
		}
		value = reflect.MakeMap(tpy)
		for i := 0; i < size; i++ {
			k, err := readArg(tpy.Key(), iprot)
			if err != nil {
				err2 = err
				break
			}
			v, err := readArg(tpy.Elem(), iprot)
			if err != nil {
				err2 = err
				break
			}
			value.SetMapIndex(k, v)
		}
		if err = iprot.ReadMapEnd(); err != nil {
			err2 = thrift.PrependError("error reading map end: ", err)
		}
	case reflect.Ptr:
		in := reflect.New(tpy.Elem())
		err = in.Interface().(ThriftReader).Read(iprot)
		if err != nil {
			err2 = err
			break
		}
		value = in
	case reflect.Slice:
		_, size, err := iprot.ReadListBegin()
		if err != nil {
			err2 = thrift.PrependError("error reading list begin: ", err)
			break
		}
		value = reflect.MakeSlice(tpy, 0, size)
		for i := 0; i < size; i++ {
			v, err := readArg(tpy.Elem(), iprot)
			if err != nil {
				err2 = err
				break
			}
			value = reflect.Append(value, v)
		}
		if err := iprot.ReadListEnd(); err != nil {
			err2 = thrift.PrependError("error reading list end: ", err)
		}
	case reflect.String:
		if in, err2 := iprot.ReadString(); err != nil {
			err = thrift.PrependError("error reading field: ", err2)

		} else {
			value = reflect.ValueOf(in)
		}
	case reflect.Struct:
		value = reflect.Zero(tpy)
		err = value.Interface().(ThriftReader).Read(iprot)
		if err != nil {
			err2 = err
			break
		}
	default:

	}
	return value, err2
}

func pack(result_name string, results []reflect.Value, oprot thrift.TProtocol) (err error) {
	if err := oprot.WriteStructBegin(result_name); err != nil {
		return thrift.PrependError(fmt.Sprintf("%s write struct begin error: ", result_name), err)
	}
	idx := 0
	if len(results) == 1 {
		idx = 1
	}
	for _, out := range results {
		if !IsNull(out) {
			if err := oprot.WriteFieldBegin("", Typmap[out.Kind()], int16(idx)); err != nil {
				return thrift.PrependError(fmt.Sprintf("write field begin error."), err)
			}
			err = writeResult(out, oprot)
			if err != nil {
				return
			}
			if err = oprot.WriteFieldEnd(); err != nil {
				return thrift.PrependError(fmt.Sprintf("write field end error."), err)
			}
		}
		idx += 1
	}
	if err := oprot.WriteFieldStop(); err != nil {
		return thrift.PrependError("write field stop error: ", err)
	}
	if err := oprot.WriteStructEnd(); err != nil {
		return thrift.PrependError("write struct stop error: ", err)
	}
	return nil
}
func writeResult(out reflect.Value, oprot thrift.TProtocol) (err error) {

	switch out.Kind() {
	case reflect.Bool:
		if err := oprot.WriteBool(out.Interface().(bool)); err != nil {
			return thrift.PrependError(fmt.Sprintf("Bool write error."), err)
		}
	case reflect.Int8:
		if err := oprot.WriteByte(out.Interface().(int8)); err != nil {
			return thrift.PrependError(fmt.Sprintf("Int8 write error."), err)
		}
	case reflect.Int16:
		if err := oprot.WriteI16(out.Interface().(int16)); err != nil {
			return thrift.PrependError(fmt.Sprintf("Int16 write error."), err)
		}
	case reflect.Int32:
		if err := oprot.WriteI32(out.Interface().(int32)); err != nil {
			return thrift.PrependError(fmt.Sprintf("Int32 write error."), err)
		}
	case reflect.Int64:
		if err := oprot.WriteI64(out.Interface().(int64)); err != nil {
			return thrift.PrependError(fmt.Sprintf("Int64 write error."), err)
		}
	case reflect.Float64:
		if err := oprot.WriteDouble(out.Interface().(float64)); err != nil {
			return thrift.PrependError(fmt.Sprintf("Float64 write error."), err)
		}
	case reflect.Map:
		if err := oprot.WriteMapBegin(Typmap[out.Type().Key().Kind()], Typmap[out.Type().Elem().Kind()], out.Len()); err != nil {
			return thrift.PrependError("error writing map begin: ", err)
		}
		for _, v := range out.MapKeys() {
			if err := writeResult(v, oprot); err != nil {
				return thrift.PrependError(fmt.Sprintf("map field write error."), err)
			}

			if err := writeResult(out.MapIndex(v), oprot); err != nil {
				return thrift.PrependError(fmt.Sprintf("map field write error."), err)
			}
		}
		if err := oprot.WriteMapEnd(); err != nil {
			return thrift.PrependError("error writing map end: ", err)
		}
	case reflect.Interface:
		fallthrough
	case reflect.Ptr:
		err := out.Interface().(ThriftWriter).Write(oprot)
		if err != nil {
			return err
		}
	case reflect.Slice:
		if err := oprot.WriteListBegin(Typmap[out.Type().Elem().Kind()], out.Len()); err != nil {
			return thrift.PrependError("error writing list begin: ", err)
		}
		for i := 0; i < out.Len(); i++ {
			if err := writeResult(out.Index(i), oprot); err != nil {
				return thrift.PrependError(fmt.Sprintf("list write error: "), err)
			}
		}
		if err := oprot.WriteListEnd(); err != nil {
			return thrift.PrependError("error writing list end: ", err)
		}
	case reflect.String:
		if err := oprot.WriteString(out.Interface().(string)); err != nil {
			return thrift.PrependError(fmt.Sprintf("String write error."), err)
		}
	case reflect.Struct:
		err := out.Interface().(ThriftWriter).Write(oprot)
		if err != nil {
			return err
		}
	default:

	}
	return nil
}

func IsNull(v reflect.Value) bool {

	switch v.Kind() {
	case reflect.Bool:
		fallthrough
	case reflect.Int:
		fallthrough
	case reflect.Int8:
		fallthrough
	case reflect.Int16:
		fallthrough
	case reflect.Int32:
		fallthrough
	case reflect.Int64:
		fallthrough
	case reflect.Uint:
		fallthrough
	case reflect.Uint8:
		fallthrough
	case reflect.Uint16:
		fallthrough
	case reflect.Uint32:
		fallthrough
	case reflect.Uint64:
		fallthrough
	case reflect.Float32:
		fallthrough
	case reflect.Float64:
		fallthrough
	case reflect.Complex64:
		fallthrough
	case reflect.Complex128:
		fallthrough
	case reflect.Array:
		fallthrough
	case reflect.String:
		fallthrough
	case reflect.Struct:
		return !v.IsValid()
	case reflect.Chan:
		fallthrough
	case reflect.Func:
		fallthrough
	case reflect.Interface:
		fallthrough
	case reflect.Map:
		fallthrough
	case reflect.Ptr:
		fallthrough
	case reflect.Uintptr:
		fallthrough
	case reflect.Slice:
		fallthrough
	case reflect.UnsafePointer:
		return v.IsNil() && v.IsValid()
	default:

	}
	return false
}
