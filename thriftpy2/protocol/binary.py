# -*- coding: utf-8 -*-

from __future__ import absolute_import

import struct

from ..thrift import TType
from .._compat import binary_to_str

from .base import TProtocolBase
from .exc import TProtocolException

# VERSION_MASK = 0xffff0000
VERSION_MASK = -65536
# VERSION_1 = 0x80010000
VERSION_1 = -2147418112
TYPE_MASK = 0x000000ff


def pack_i8(byte):
    return struct.pack("!b", byte)


def pack_i16(i16):
    return struct.pack("!h", i16)


def pack_i32(i32):
    return struct.pack("!i", i32)


def pack_i64(i64):
    return struct.pack("!q", i64)


def pack_double(dub):
    return struct.pack("!d", dub)


def pack_string(string):
    return struct.pack("!i%ds" % len(string), len(string), string)


def unpack_i8(buf):
    return struct.unpack("!b", buf)[0]


def unpack_i16(buf):
    return struct.unpack("!h", buf)[0]


def unpack_i32(buf):
    return struct.unpack("!i", buf)[0]


def unpack_i64(buf):
    return struct.unpack("!q", buf)[0]


def unpack_double(buf):
    return struct.unpack("!d", buf)[0]


class TBinaryProtocol(TProtocolBase):
    """Binary implementation of the Thrift protocol driver."""

    def __init__(self, trans,
                 strict_read=True, strict_write=True,
                 decode_response=True):
        super(TBinaryProtocol, self).__init__(trans, decode_response)
        self.strict_read = strict_read
        self.strict_write = strict_write

    def read_bool(self):
        return bool(self.read_byte())

    def read_byte(self):
        return unpack_i8(self.trans.read(1))

    def read_double(self):
        return unpack_double(self.trans.read(8))

    def read_i16(self):
        return unpack_i16(self.trans.read(2))

    def read_i32(self):
        return unpack_i32(self.trans.read(4))

    def read_i64(self):
        return unpack_i64(self.trans.read(8))

    def read_binary(self):
        size = self.read_i32()
        return self.trans.read(size)

    def read_map_begin(self):
        ktype, vtype = self.read_byte(), self.read_byte()
        sz = self.read_i32()
        return ktype, vtype, sz

    def read_list_begin(self):
        etype = self.read_byte()
        sz = self.read_i32()
        return etype, sz

    read_set_begin = read_list_begin

    def read_field_begin(self):
        ftype = self.read_byte()
        if ftype == TType.STOP:
            return None, ftype, 0

        return None, ftype, self.read_i16()

    def read_message_begin(self):
        sz = self.read_i32()
        if sz < 0:
            version = sz & VERSION_MASK
            if version != VERSION_1:
                raise TProtocolException(
                    type=TProtocolException.BAD_VERSION,
                    message='Bad version in read_message_begin: %d' % (sz))
            name = self.read_string()
            type_ = sz & TYPE_MASK
        else:
            if self.strict_read:
                raise TProtocolException(type=TProtocolException.BAD_VERSION,
                                         message='No protocol version header')

            name = self.trans.read(sz)
            if self.decode_response:
                name = binary_to_str(name)
            type_ = self.read_byte()

        seqid = self.read_i32()
        return name, type_, seqid

    def write_bool(self, bool):
        self.write_byte(1 if bool else 0)

    def write_byte(self, byte):
        self.trans.write(pack_i8(byte))

    def write_double(self, dub):
        self.trans.write(pack_double(dub))

    def write_i16(self, i16):
        self.trans.write(pack_i16(i16))

    def write_i32(self, i32):
        self.trans.write(pack_i32(i32))

    def write_i64(self, i64):
        self.trans.write(pack_i64(i64))

    def write_binary(self, str_val):
        self.trans.write(pack_string(str_val))

    def write_map_begin(self, ktype, vtype, size):
        self.trans.write(pack_i8(ktype) + pack_i8(vtype) + pack_i32(size))

    def write_list_begin(self, etype, size):
        self.trans.write(pack_i8(etype) + pack_i32(size))

    def write_field_begin(self, name, ttype, fid):
        self.trans.write(pack_i8(ttype) + pack_i16(fid))

    def write_field_stop(self):
        self.write_byte(TType.STOP)

    def write_message_begin(self, name, ttype, seqid):
        if self.strict_write:
            self.write_i32(VERSION_1 | ttype)
            self.write_string(name)
        else:
            self.write_string(name)
            self.write_byte(ttype)

        self.write_i32(seqid)


class TBinaryProtocolFactory(object):
    def __init__(self, strict_read=True, strict_write=True,
                 decode_response=True):
        self.strict_read = strict_read
        self.strict_write = strict_write
        self.decode_response = decode_response

    def get_protocol(self, trans):
        return TBinaryProtocol(trans,
                               self.strict_read, self.strict_write,
                               self.decode_response)
