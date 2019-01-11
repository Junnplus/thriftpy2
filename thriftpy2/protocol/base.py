# -*- coding: utf-8 -*-

from __future__ import absolute_import

from .._compat import binary_to_str, str_to_binary
from ..thrift import TType


class TProtocolBase(object):
    """Base class for Thrift protocol driver."""

    _BASE_TTYPE = (TType.BOOL, TType.BYTE, TType.I16, TType.I32,
                   TType.I64, TType.DOUBLE, TType.STRING)
    _CONTAINER_TTYPE = (TType.LIST, TType.SET, TType.MAP, TType.STRUCT)

    def __init__(self, trans, decode_response=True):
        self.trans = trans
        self.decode_response = decode_response

    def read_bool(self):
        pass

    def read_byte(self):
        pass

    def read_double(self):
        pass

    def read_i16(self):
        pass

    def read_i32(self):
        pass

    def read_i64(self):
        pass

    def read_string(self):
        str_ = self.read_binary()

        if self.decode_response:
            return binary_to_str(str_)
        return str_

    def read_binary(self):
        pass

    def read_struct_begin(self):
        pass

    def read_struct_end(self):
        pass

    def read_struct(self, obj):
        self.read_struct_begin()
        while True:
            _, f_type, fid = self.read_field_begin()
            if f_type == TType.STOP:
                break

            if fid not in obj.thrift_spec:
                self.skip(f_type)
                continue

            try:
                fspec = obj.thrift_spec[fid]
            except IndexError:
                self.skip(f_type)
                raise

            if len(fspec) == 3:
                sf_type, f_name, _ = fspec
                f_container_spec = None
            else:
                sf_type, f_name, f_container_spec, _ = fspec

            # it really should equal here. but since we already wasted
            # space storing the duplicate info, let's check it.
            if f_type != sf_type:
                self.skip(f_type)
                continue

            val = self.read_val(f_type, f_container_spec)
            setattr(obj, f_name, val)

            self.read_field_end()
        self.read_struct_end()

    def read_map_begin(self):
        pass

    def read_map_end(self):
        pass

    def read_map(self, spec):
        if isinstance(spec[0], int):
            k_type = spec[0]
            k_spec = None
        else:
            k_type, k_spec = spec[0]

        if isinstance(spec[1], int):
            v_type = spec[1]
            v_spec = None
        else:
            v_type, v_spec = spec[1]

        sk_type, sv_type, sz = self.read_map_begin()
        if sk_type != k_type or sv_type != v_type:
            for _ in range(sz):
                self.skip(sk_type)
                self.skip(sv_type)
            return {}

        result = {}
        for i in range(sz):
            k_val = self.read_val(k_type, k_spec)
            v_val = self.read_val(v_type, v_spec)
            result[k_val] = v_val
        return result

    def read_set_begin(self):
        pass

    def read_set_end(self):
        pass

    def read_set(self, val, spec):
        if isinstance(spec, tuple):
            v_type, v_spec = spec[0], spec[1]
        else:
            v_type, v_spec = spec, None

        r_type, sz = self.read_set_begin()
        # the v_type is useless here since we already get it from spec
        if r_type != v_type:
            for _ in range(sz):
                self.skip(r_type)
            return []

        result = []
        for i in range(sz):
            result.append(self.read_val(v_type, v_spec))

        self.read_set_end()
        return result

    def read_list_begin(self):
        pass

    def read_list_end(self):
        pass

    def read_list(self, spec):
        if isinstance(spec, tuple):
            v_type, v_spec = spec[0], spec[1]
        else:
            v_type, v_spec = spec, None

        r_type, sz = self.read_list_begin()
        # the v_type is useless here since we already get it from spec
        if r_type != v_type:
            for _ in range(sz):
                self.skip(r_type)
            return []

        result = []
        for i in range(sz):
            result.append(self.read_val(v_type, v_spec))

        self.read_list_end()
        return result

    def read_field_begin(self):
        pass

    def read_field_end(self):
        pass

    def read_message_begin(self):
        pass

    def read_message_end(self):
        pass

    def read_val(self, ttype, spec=None):
        if ttype not in self._BASE_TTYPE + self._CONTAINER_TTYPE:
            return

        ttype_name = TType._VALUES_TO_NAMES[ttype].lower()
        read_func = getattr(self, 'read_' + ttype_name)
        if not read_func:
            return

        if ttype in self._BASE_TTYPE:
            return read_func()

        elif ttype == TType.STRUCT:
            obj = spec()
            read_func(obj)
            return obj

        elif ttype in self._CONTAINER_TTYPE:
            return read_func(spec)

    def write_bool(self, bool):
        pass

    def write_byte(self, byte):
        pass

    def write_double(self, dub):
        pass

    def write_i16(self, i16):
        pass

    def write_i32(self, i32):
        pass

    def write_i64(self, i64):
        pass

    def write_string(self, str_val):
        self.write_binary(str_to_binary(str_val))

    def write_binary(self, str_val):
        pass

    def write_struct_begin(self):
        pass

    def write_struct_end(self):
        pass

    def write_struct(self, obj, spec=None):
        self.write_struct_begin()

        for fid, fspec in obj.thrift_spec.items():
            if fid is None:
                continue

            if len(fspec) == 3:
                f_type, f_name, _ = fspec
                f_container_spec = None
            else:
                f_type, f_name, f_container_spec, _ = fspec

            val = getattr(obj, f_name)
            if val is None:
                continue

            self.write_field_begin(f_name, f_type, fid)
            self.write_val(f_type, val, f_container_spec)
            self.write_field_end()
        self.write_field_stop()
        self.write_struct_end()

    def write_map_begin(self, ktype, vtype, size):
        pass

    def write_map_end(self):
        pass

    def write_map(self, val, spec):
        if isinstance(spec[0], int):
            k_type = spec[0]
            k_spec = None
        else:
            k_type, k_spec = spec[0]

        if isinstance(spec[1], int):
            v_type = spec[1]
            v_spec = None
        else:
            v_type, v_spec = spec[1]

        self.write_map_begin(k_type, v_type, len(val))
        for k in iter(val):
            self.write_val(k_type, k, k_spec)
            self.write_val(v_type, val[k], v_spec)

    def write_set_begin(self, etype, size):
        pass

    def write_set_end(self):
        pass

    def write_set(self, val, spec):
        if isinstance(spec, tuple):
            e_type, t_spec = spec[0], spec[1]
        else:
            e_type, t_spec = spec, None

        val_len = len(val)
        self.write_set_begin(e_type, val_len)
        for e_val in val:
            self.write_val(e_type, e_val, t_spec)
        self.write_set_end()

    def write_list_begin(self, etype, size):
        pass

    def write_list_end(self):
        pass

    def write_list(self, val, spec):
        if isinstance(spec, tuple):
            e_type, t_spec = spec[0], spec[1]
        else:
            e_type, t_spec = spec, None

        val_len = len(val)
        self.write_list_begin(e_type, val_len)
        for e_val in val:
            self.write_val(e_type, e_val, t_spec)
        self.write_list_end()

    def write_field_begin(self, name, ttype, fid):
        pass

    def write_field_end(self):
        pass

    def write_field_stop(self):
        pass

    def write_message_begin(self, name, ttype, seqid):
        pass

    def write_message_end(self):
        pass

    def write_val(self, ttype, val, spec=None):
        if ttype not in self._BASE_TTYPE + self._CONTAINER_TTYPE:
            return

        ttype_name = TType._VALUES_TO_NAMES[ttype].lower()
        write_func = getattr(self, 'write_' + ttype_name)
        if not write_func:
            return

        if ttype in self._BASE_TTYPE:
            write_func(val)

        elif ttype in self._CONTAINER_TTYPE:
            write_func(val, spec)

    def skip(self, ttype):
        if ttype == TType.STOP:
            return

        elif ttype == TType.BOOL:
            self.read_bool()

        elif ttype == TType.BYTE:
            self.read_byte()

        elif ttype == TType.I16:
            self.read_i16()

        elif ttype == TType.I32:
            self.read_i32()

        elif ttype == TType.I64:
            self.read_i64()

        elif ttype == TType.DOUBLE:
            self.read_double()

        elif ttype == TType.STRING:
            self.read_string()

        elif ttype == TType.STRUCT:
            self.read_struct_begin()
            while True:
                _, ftype, _ = self.read_field_begin()
                if ftype == TType.STOP:
                    break
                self.skip(ftype)
                self.read_field_end()
            self.read_struct_end()

        elif ttype == TType.MAP:
            ktype, vtype, size = self.read_map_begin()
            for i in range(size):
                self.skip(ktype)
                self.skip(vtype)
            self.read_map_end()

        elif ttype == TType.SET:
            etype, size = self.read_set_begin()
            for i in range(size):
                self.skip(etype)
            self.read_set_end()

        elif ttype == TType.LIST:
            etype, size = self.read_list_begin()
            for i in range(size):
                self.skip(etype)
            self.read_list_end()
