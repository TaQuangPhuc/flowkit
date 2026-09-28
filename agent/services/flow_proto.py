"""Minimal protobuf wire codec for Flow gRPC payloads.

Fields decode to (fnum, wiretype, value); varint -> int, wire2/5/1 -> bytes.
Nested messages stay raw and are decoded on demand. Same codec as the
research tooling (.omc/research/proto_lib.py), vendored so the agent does
not import from the research tree.
"""
from __future__ import annotations


def varint(n: int) -> bytes:
    out = bytearray()
    while n > 0x7F:
        out.append((n & 0x7F) | 0x80)
        n >>= 7
    out.append(n)
    return bytes(out)


def decode(buf: bytes) -> list[tuple[int, int, object]]:
    fields = []
    i, n = 0, len(buf)
    while i < n:
        tag = shift = 0
        while True:
            tag |= (buf[i] & 0x7F) << shift
            shift += 7
            if not buf[i] & 0x80:
                i += 1
                break
            i += 1
        fnum, wire = tag >> 3, tag & 7
        if wire == 0:
            v = shift = 0
            while True:
                v |= (buf[i] & 0x7F) << shift
                shift += 7
                if not buf[i] & 0x80:
                    i += 1
                    break
                i += 1
            fields.append((fnum, wire, v))
        elif wire == 2:
            ln = shift = 0
            while True:
                ln |= (buf[i] & 0x7F) << shift
                shift += 7
                if not buf[i] & 0x80:
                    i += 1
                    break
                i += 1
            fields.append((fnum, wire, bytes(buf[i:i + ln])))
            i += ln
        elif wire == 5:
            fields.append((fnum, wire, bytes(buf[i:i + 4])))
            i += 4
        elif wire == 1:
            fields.append((fnum, wire, bytes(buf[i:i + 8])))
            i += 8
        else:
            raise ValueError(f"wire {wire} at {i}")
    return fields


def encode(fields) -> bytes:
    out = bytearray()
    for fnum, wire, val in fields:
        out += varint((fnum << 3) | wire)
        if wire == 0:
            out += varint(val)
        elif wire == 2:
            out += varint(len(val)) + val
        elif wire == 5:
            out += val
        elif wire == 1:
            out += val
    return bytes(out)


def set_leaf(fields, path: list[int], new_bytes: bytes) -> bytes:
    """Replace the wire-2 field at nested `path`; returns encoded bytes."""
    fnum = path[0]
    out = []
    done = False
    for f, w, v in fields:
        if f == fnum and w == 2 and not done:
            if len(path) == 1:
                out.append((f, w, new_bytes))
            else:
                out.append((f, w, set_leaf(decode(v), path[1:], new_bytes)))
            done = True
        else:
            out.append((f, w, v))
    return encode(out)


def set_leaf_bytes(top_bytes: bytes, path: list[int], new_bytes: bytes) -> bytes:
    return set_leaf(decode(top_bytes), path, new_bytes)


def set_varint(top_bytes: bytes, path: list[int], n: int) -> bytes:
    """Replace the first wire-0 varint field at nested `path`."""
    fnum = path[0]
    out = []
    done = False
    for f, w, v in decode(top_bytes):
        if f == fnum and not done:
            if len(path) == 1 and w == 0:
                out.append((f, w, n))
                done = True
            elif len(path) > 1 and w == 2:
                out.append((f, w, set_varint(v, path[1:], n)))
                done = True
            else:
                out.append((f, w, v))
        else:
            out.append((f, w, v))
    return encode(out)


def replace_fields(top_bytes: bytes, path: list[int], fnum: int,
                   payloads: list[bytes]) -> bytes:
    """Inside the msg at `path`, drop every wire-2 field `fnum` and insert
    `payloads` in their place (first occurrence position)."""
    if not path:
        out, inserted = [], False
        for f, w, v in decode(top_bytes):
            if f == fnum and w == 2:
                if not inserted:
                    out += [(fnum, 2, p) for p in payloads]
                    inserted = True
            else:
                out.append((f, w, v))
        if not inserted:
            out += [(fnum, 2, p) for p in payloads]
        return encode(out)
    head = path[0]
    out = []
    done = False
    for f, w, v in decode(top_bytes):
        if f == head and w == 2 and not done:
            out.append((f, w, replace_fields(v, path[1:], fnum, payloads)))
            done = True
        else:
            out.append((f, w, v))
    return encode(out)


def fld_varint(fnum: int, n: int) -> bytes:
    return varint((fnum << 3) | 0) + varint(n)


def fld_msg(fnum: int, payload: bytes) -> bytes:
    return varint((fnum << 3) | 2) + varint(len(payload)) + payload


def fld_str(fnum: int, s) -> bytes:
    b = s.encode("utf-8") if isinstance(s, str) else s
    return fld_msg(fnum, b)


def walk_leaves(buf: bytes, path=None, out=None):
    """Yield (path, value) for every leaf; wire-2 values that decode cleanly
    recurse, others are leaves. A bytes leaf is yielded only if decoding it
    produces no fields."""
    path = path or []
    out = out if out is not None else []
    for fnum, wire, val in decode(buf):
        p = path + [fnum]
        if wire == 2 and isinstance(val, bytes) and len(val) > 2:
            try:
                sub = decode(val)
            except Exception:
                sub = []
            if sub:
                walk_leaves(val, p, out)
                continue
        out.append((p, val))
    return out
