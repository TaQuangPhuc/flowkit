// h2cronet — minimal HTTP/2 client transport that mimics Chromium's
// (Cronet) wire behavior: identical SETTINGS payload, the 16 MiB
// connection WINDOW_UPDATE, and Chromium header ordering — so the
// server's h2 fingerprint sees the same stack an iOS Flow app uses.
//
// Only what's needed for unary gRPC/REST over TLS via the relay:
// one conn per (nick, host), odd stream ids, HEADERS+DATA out,
// HEADERS+DATA+trailers in, flow control honored.
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/textproto"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/http2/hpack"
)

// ---- frame codec -----------------------------------------------------------

type h2Frame struct {
	typ     byte
	flags   byte
	stream  uint32
	payload []byte
	flowLen int // original payload length, including padding (flow-control)
}

func readFrame(r io.Reader) (h2Frame, error) {
	var f h2Frame
	var h [9]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return f, err
	}
	n := int(h[0])<<16 | int(h[1])<<8 | int(h[2])
	f.typ, f.flags = h[3], h[4]
	f.stream = uint32(h[5]&0x7f)<<24 | uint32(h[6])<<16 |
		uint32(h[7])<<8 | uint32(h[8])
	f.flowLen = n
	f.payload = make([]byte, n)
	_, err := io.ReadFull(r, f.payload)
	if err != nil {
		return f, err
	}
	// strip padding (flag 0x08): payload = padlen(1B) + data + pad
	if f.flags&flagPadded != 0 && (f.typ == fData || f.typ == fHeaders) &&
		len(f.payload) > 0 {
		padLen := int(f.payload[0])
		if padLen+1 <= len(f.payload) {
			f.payload = f.payload[1 : len(f.payload)-padLen]
		}
	}
	return f, err
}

var debugH2 = os.Getenv("H2_DEBUG") == "1"

func writeFrame(w io.Writer, typ, flags byte, stream uint32, p []byte) error {
	if debugH2 {
		log.Printf("h2>> type=%02x flags=%02x stream=%d len=%d", typ, flags,
			stream, len(p))
	}
	h := [9]byte{
		byte(len(p) >> 16), byte(len(p) >> 8), byte(len(p)),
		typ, flags,
		byte(stream >> 24), byte(stream >> 16), byte(stream >> 8),
		byte(stream),
	}
	if _, err := w.Write(h[:]); err != nil {
		return err
	}
	_, err := w.Write(p)
	return err
}

const (
	fData     = 0x0
	fHeaders  = 0x1
	fPriority = 0x2
	fRst      = 0x3
	fSettings = 0x4
	fPush     = 0x5
	fPing     = 0x6
	fGoaway   = 0x7
	fWindow   = 0x8
	fContin   = 0x9
)

const (
	flagEndStream  = 0x1
	flagAck        = 0x1
	flagEndHeaders = 0x4
	flagPadded     = 0x8
	flagPriority   = 0x20
)

// ---- Cronet/Chromium wire profile ------------------------------------------
// Chromium sends:
//
//	SETTINGS  HEADER_TABLE_SIZE=65536  MAX_CONCURRENT_STREAMS=1000
//	          INITIAL_WINDOW_SIZE=6291456  MAX_HEADER_LIST_SIZE=262144
//	WINDOW_UPDATE +15663105  (raises conn window to ~16 MiB)
var cronetSettings = []byte{
	0, 1, 0, 0, 1, 0, // HEADER_TABLE_SIZE 65536
	0, 3, 0, 0, 3, 232, // MAX_CONCURRENT_STREAMS 1000
	0, 4, 0, 96, 0, 0, // INITIAL_WINDOW_SIZE 6291456
	0, 6, 0, 4, 0, 0, // MAX_HEADER_LIST_SIZE 262144
}
var cronetWindow = []byte{0, 239, 0, 1} // +15663105

// spec default until the peer sends WINDOW_UPDATE / SETTINGS.
const defaultStreamWindow int64 = 65535

// ---- connection ------------------------------------------------------------

type h2conn struct {
	conn   net.Conn
	mu     sync.Mutex // serializes frame writes
	nextID uint32
	closed atomic.Bool

	// How much WE may send. Starts at the spec default 65535.
	// 6291456 in cronetSettings is the window we grant the peer, not this.
	sendMu         sync.Mutex
	sendCond       *sync.Cond
	sendConn       int64
	peerStreamInit int64
	peerMaxFrame   int

	streams sync.Map // uint32 → *h2stream

	dec    *hpack.Decoder
	decMu  sync.Mutex
	enc    *hpack.Encoder
	encBuf bytes.Buffer
	encMu  sync.Mutex

	dialTLS func(ctx context.Context, addr string) (net.Conn, error)
	addr    string
}

type h2ev struct {
	kind byte // H headers, D data, T trailers, E error
	end  bool
	f    h2Frame
	err  error
}

// Events queue without bound: readLoop serves every stream on the conn, so
// it must never block on one slow reader, and a dropped DATA frame used to
// stall the stream silently until the 120s deadline. Memory is bounded by
// the response size, which roundTrip buffers in full anyway.
type h2stream struct {
	id     uint32
	qmu    sync.Mutex
	queue  []h2ev
	notify chan struct{} // cap 1: "queue became non-empty"
	ended  atomic.Bool
	failed atomic.Bool
	errMu  sync.Mutex
	err    error

	sendWin int64
}

func newStream(id uint32) *h2stream {
	return &h2stream{id: id, notify: make(chan struct{}, 1)}
}

func (s *h2stream) push(ev h2ev) {
	s.qmu.Lock()
	s.queue = append(s.queue, ev)
	s.qmu.Unlock()
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func (s *h2stream) drain() []h2ev {
	s.qmu.Lock()
	q := s.queue
	s.queue = nil
	s.qmu.Unlock()
	return q
}

func (s *h2stream) getErr() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	if s.err != nil {
		return s.err
	}
	return fmt.Errorf("stream failed")
}

func (s *h2stream) fail(err error) {
	if s.ended.Load() || err == nil {
		return
	}
	if !s.failed.CompareAndSwap(false, true) {
		return
	}
	s.errMu.Lock()
	s.err = err
	s.errMu.Unlock()
	s.push(h2ev{kind: 'E', err: err})
}

func (c *h2conn) wakeSenders() {
	c.sendMu.Lock()
	c.sendCond.Broadcast()
	c.sendMu.Unlock()
}

func (c *h2conn) markClosed() {
	c.closed.Store(true)
	c.wakeSenders()
}

func (c *h2conn) writeFrame(typ, flags byte, stream uint32, p []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return writeFrame(c.conn, typ, flags, stream, p)
}

func (c *h2conn) readLoop() {
	defer c.markClosed()
	pending := map[uint32]*h2Frame{}
	// headerSeen is per stream: the first HEADERS block is the response,
	// a later one is trailers (even when the first also has END_STREAM).
	sawHeader := map[uint32]bool{}
	emitHeaders := func(f h2Frame) {
		v, ok := c.streams.Load(f.stream)
		if !ok {
			return
		}
		s := v.(*h2stream)
		end := f.flags&flagEndStream != 0
		kind := byte('H')
		if sawHeader[f.stream] {
			kind = 'T'
		} else {
			sawHeader[f.stream] = true
		}
		if end {
			s.ended.Store(true)
		}
		s.push(h2ev{kind: kind, end: end, f: f})
		if end {
			c.wakeSenders()
		}
	}
	for {
		f, err := readFrame(c.conn)
		if debugH2 && err == nil {
			log.Printf("h2<< type=%02x flags=%02x stream=%d len=%d", f.typ,
				f.flags, f.stream, len(f.payload))
		}
		if err != nil {
			c.streams.Range(func(_, v any) bool {
				v.(*h2stream).fail(fmt.Errorf("conn read: %w", err))
				return true
			})
			return
		}
		switch f.typ {
		case fSettings:
			if f.flags&flagAck == 0 {
				c.applySettings(f.payload)
				if err := c.writeFrame(fSettings, flagAck, 0, nil); err != nil {
					c.streams.Range(func(_, v any) bool {
						v.(*h2stream).fail(err)
						return true
					})
					return
				}
			}
		case fPing:
			if f.flags&flagAck == 0 {
				if err := c.writeFrame(fPing, flagAck, 0, f.payload); err != nil {
					return
				}
			}
		case fWindow:
			if len(f.payload) < 4 {
				continue
			}
			inc := int64(uint32(f.payload[0]&0x7f)<<24 |
				uint32(f.payload[1])<<16 | uint32(f.payload[2])<<8 |
				uint32(f.payload[3]))
			c.sendMu.Lock()
			if f.stream == 0 {
				c.sendConn += inc
			} else if v, ok := c.streams.Load(f.stream); ok {
				v.(*h2stream).sendWin += inc
			}
			c.sendCond.Broadcast()
			c.sendMu.Unlock()
		case fHeaders:
			if f.flags&flagEndHeaders == 0 {
				cp := f
				pending[f.stream] = &cp
				continue
			}
			emitHeaders(f)
		case fContin:
			pb, ok := pending[f.stream]
			if !ok {
				continue
			}
			pb.payload = append(pb.payload, f.payload...)
			if f.flags&flagEndHeaders != 0 {
				fin := *pb
				delete(pending, f.stream)
				emitHeaders(fin)
			}
		case fData:
			v, ok := c.streams.Load(f.stream)
			if ok {
				s := v.(*h2stream)
				end := f.flags&flagEndStream != 0
				if end {
					s.ended.Store(true)
				}
				s.push(h2ev{kind: 'D', end: end, f: f})
				if end {
					c.wakeSenders()
				}
			}
			// Return credit so the peer can keep sending. Size includes padding.
			if f.flowLen > 0 {
				upd := []byte{
					byte(f.flowLen >> 24), byte(f.flowLen >> 16),
					byte(f.flowLen >> 8), byte(f.flowLen)}
				_ = c.writeFrame(fWindow, 0, f.stream, upd)
				_ = c.writeFrame(fWindow, 0, 0, upd)
			}
		case fRst:
			if v, ok := c.streams.Load(f.stream); ok {
				s := v.(*h2stream)
				code := uint32(0)
				if len(f.payload) >= 4 {
					code = uint32(f.payload[0])<<24 |
						uint32(f.payload[1])<<16 |
						uint32(f.payload[2])<<8 | uint32(f.payload[3])
				}
				// Google sends RST NO_ERROR after a finished response.
				if !s.ended.Load() {
					s.fail(fmt.Errorf("RST_STREAM code=%d", code))
					c.wakeSenders()
				}
			}
		case fGoaway:
			code := uint32(0)
			if len(f.payload) >= 8 {
				code = uint32(f.payload[4])<<24 | uint32(f.payload[5])<<16 |
					uint32(f.payload[6])<<8 | uint32(f.payload[7])
			}
			debug := ""
			if len(f.payload) > 8 {
				debug = string(f.payload[8:])
			}
			err := fmt.Errorf("GOAWAY code=%d %s", code, debug)
			c.streams.Range(func(_, v any) bool {
				v.(*h2stream).fail(err)
				return true
			})
			c.wakeSenders()
			return
		}
	}
}

func (c *h2conn) applySettings(p []byte) {
	for i := 0; i+6 <= len(p); i += 6 {
		id := uint16(p[i])<<8 | uint16(p[i+1])
		val := uint32(p[i+2])<<24 | uint32(p[i+3])<<16 |
			uint32(p[i+4])<<8 | uint32(p[i+5])
		switch id {
		case 1: // HEADER_TABLE_SIZE
			c.encMu.Lock()
			c.enc.SetMaxDynamicTableSize(val)
			c.encMu.Unlock()
		case 4: // INITIAL_WINDOW_SIZE — peer's limit on what we send per stream
			if val > 0x7fffffff {
				val = 0x7fffffff
			}
			c.sendMu.Lock()
			delta := int64(val) - c.peerStreamInit
			c.peerStreamInit = int64(val)
			c.streams.Range(func(_, v any) bool {
				v.(*h2stream).sendWin += delta
				return true
			})
			c.sendCond.Broadcast()
			c.sendMu.Unlock()
		case 5: // MAX_FRAME_SIZE
			if val >= 16384 && val <= 16777215 {
				c.sendMu.Lock()
				c.peerMaxFrame = int(val)
				c.sendMu.Unlock()
			}
		}
	}
}

// ---- round trip ------------------------------------------------------------

type cronetTransport struct {
	dialTLS func(ctx context.Context, addr string) (net.Conn, error)
	mu      sync.Mutex
	conns   map[string]*h2conn
}

func (t *cronetTransport) connFor(ctx context.Context, addr string) (*h2conn, error) {
	t.mu.Lock()
	if t.conns == nil {
		t.conns = map[string]*h2conn{}
	}
	if c, ok := t.conns[addr]; ok && !c.closed.Load() {
		t.mu.Unlock()
		return c, nil
	}
	t.mu.Unlock()

	raw, err := t.dialTLS(ctx, addr)
	if err != nil {
		return nil, err
	}
	c := &h2conn{
		conn:           raw,
		nextID:         ^uint32(0),
		sendConn:       defaultStreamWindow,
		peerStreamInit: defaultStreamWindow,
		peerMaxFrame:   16384,
		dec:            hpack.NewDecoder(65536, nil),
		dialTLS:        t.dialTLS,
		addr:           addr,
	}
	c.sendCond = sync.NewCond(&c.sendMu)
	c.enc = hpack.NewEncoder(&c.encBuf)
	if _, err := raw.Write([]byte("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")); err != nil {
		raw.Close()
		return nil, err
	}
	if err := writeFrame(raw, fSettings, 0, 0, cronetSettings); err != nil {
		raw.Close()
		return nil, err
	}
	if err := writeFrame(raw, fWindow, 0, 0, cronetWindow); err != nil {
		raw.Close()
		return nil, err
	}
	go c.readLoop()

	t.mu.Lock()
	if cur, ok := t.conns[addr]; ok && !cur.closed.Load() {
		t.mu.Unlock()
		c.closed.Store(true)
		raw.Close()
		return cur, nil
	}
	t.conns[addr] = c
	t.mu.Unlock()
	return c, nil
}

func (c *h2conn) encodeHeadersLocked(req *http.Request) ([]byte, error) {
	c.encBuf.Reset()
	ps := [][2]string{
		{":method", req.Method},
		{":authority", req.URL.Host},
		{":scheme", "https"},
		{":path", req.URL.RequestURI()},
	}
	for _, kv := range ps {
		if err := c.enc.WriteField(hpack.HeaderField{
			Name: kv[0], Value: kv[1]}); err != nil {
			return nil, err
		}
	}
	skip := map[string]bool{"host": true, "connection": true,
		"content-length": true, "transfer-encoding": true}
	names := make([]string, 0, len(req.Header))
	for name := range req.Header {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		ln := strings.ToLower(name)
		if skip[ln] {
			continue
		}
		for _, v := range req.Header[name] {
			if err := c.enc.WriteField(hpack.HeaderField{
				Name: ln, Value: v}); err != nil {
				return nil, err
			}
		}
	}
	return append([]byte(nil), c.encBuf.Bytes()...), nil
}

// beginStream allocates the next odd stream id and writes HEADERS before
// releasing the write lock. HTTP/2 requires stream ids to hit the wire in
// increasing order; allocating and writing separately races under load.
func (c *h2conn) beginStream(req *http.Request, hasBody bool) (*h2stream, error) {
	c.encMu.Lock()
	defer c.encMu.Unlock()
	hdrs, err := c.encodeHeadersLocked(req)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	id := atomic.AddUint32(&c.nextID, 2)
	s := newStream(id)
	c.streams.Store(id, s)
	end := byte(0)
	if !hasBody {
		end = flagEndStream
	}
	err = writeFrame(c.conn, fHeaders, flagEndHeaders|end, id, hdrs)
	c.mu.Unlock()
	if err != nil {
		s.fail(err)
		return nil, err
	}
	c.sendMu.Lock()
	s.sendWin = c.peerStreamInit
	c.sendCond.Broadcast()
	c.sendMu.Unlock()
	return s, nil
}

func (c *h2conn) writeData(s *h2stream, p []byte, end bool) error {
	for {
		if s.failed.Load() {
			return s.getErr()
		}
		c.sendMu.Lock()
		for len(p) > 0 && (s.sendWin <= 0 || c.sendConn <= 0) {
			if c.closed.Load() || s.failed.Load() {
				c.sendMu.Unlock()
				if s.failed.Load() {
					return s.getErr()
				}
				return fmt.Errorf("conn closed while sending body")
			}
			c.sendCond.Wait()
		}
		if len(p) == 0 {
			c.sendMu.Unlock()
			if end {
				return c.writeFrame(fData, flagEndStream, s.id, nil)
			}
			return nil
		}
		n := len(p)
		maxF := c.peerMaxFrame
		if maxF < 16384 {
			maxF = 16384
		}
		if n > maxF {
			n = maxF
		}
		if int64(n) > s.sendWin {
			n = int(s.sendWin)
		}
		if int64(n) > c.sendConn {
			n = int(c.sendConn)
		}
		if n <= 0 {
			c.sendCond.Wait()
			c.sendMu.Unlock()
			continue
		}
		last := end && n == len(p)
		s.sendWin -= int64(n)
		c.sendConn -= int64(n)
		chunk := p[:n]
		p = p[n:]
		c.sendMu.Unlock()
		flags := byte(0)
		if last {
			flags = flagEndStream
		}
		if err := c.writeFrame(fData, flags, s.id, chunk); err != nil {
			return err
		}
		if last {
			return nil
		}
	}
}

func (c *h2conn) sendBody(s *h2stream, r io.Reader) error {
	buf := make([]byte, 16384)
	var pend []byte
	for {
		if s.failed.Load() {
			return s.getErr()
		}
		n, err := r.Read(buf)
		if n > 0 {
			if len(pend) > 0 {
				if e := c.writeData(s, pend, false); e != nil {
					return e
				}
			}
			pend = append([]byte(nil), buf[:n]...)
		}
		if err == io.EOF {
			return c.writeData(s, pend, true)
		}
		if err != nil {
			return err
		}
	}
}

func applyFields(h http.Header, fields []hpack.HeaderField) (status int) {
	for _, f := range fields {
		if f.Name == ":status" {
			status, _ = strconv.Atoi(f.Value)
			continue
		}
		h.Add(textproto.CanonicalMIMEHeaderKey(f.Name), f.Value)
	}
	return status
}

func (c *h2conn) roundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	hasBody := req.Body != nil && req.ContentLength != 0
	s, err := c.beginStream(req, hasBody)
	if err != nil {
		return nil, err
	}
	defer c.streams.Delete(s.id)
	stop := context.AfterFunc(ctx, func() {
		s.fail(ctx.Err())
		c.wakeSenders()
		_ = c.writeFrame(fRst, 0, s.id, []byte{0, 0, 0, 8}) // CANCEL
	})
	defer stop()
	if hasBody {
		if err := c.sendBody(s, req.Body); err != nil {
			return nil, err
		}
	}

	var resp *http.Response
	var bodyBuf bytes.Buffer
	var trailers http.Header
	headerSeen := false
	decode := func(p []byte) ([]hpack.HeaderField, error) {
		c.decMu.Lock()
		defer c.decMu.Unlock()
		return c.dec.DecodeFull(p)
	}
	deadline := time.NewTimer(120 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline.C:
			return nil, fmt.Errorf("h2 response timeout")
		case <-s.notify:
			for _, ev := range s.drain() {
				switch ev.kind {
				case 'E':
					return nil, ev.err
				case 'H':
					fields, err := decode(ev.f.payload)
					if err != nil {
						return nil, err
					}
					resp = &http.Response{
						Proto: "HTTP/2.0", ProtoMajor: 2,
						Header: http.Header{}, Trailer: http.Header{},
					}
					resp.StatusCode = applyFields(resp.Header, fields)
					resp.Status = strconv.Itoa(resp.StatusCode) + " " +
						http.StatusText(resp.StatusCode)
					headerSeen = true
					if ev.end {
						for k, vs := range resp.Header {
							if strings.HasPrefix(k, "Grpc-") {
								for _, v := range vs {
									resp.Trailer.Add(k, v)
								}
							}
						}
						goto finish
					}
				case 'D':
					bodyBuf.Write(ev.f.payload)
					if ev.end {
						goto finish
					}
				case 'T':
					fields, err := decode(ev.f.payload)
					if err != nil {
						return nil, err
					}
					if trailers == nil {
						trailers = http.Header{}
					}
					applyFields(trailers, fields)
					goto finish
				}
			}
		}
	}
finish:
	if !headerSeen || resp == nil {
		return nil, fmt.Errorf("no response headers")
	}
	resp.Header.Del("Content-Length")
	resp.Header.Del("Transfer-Encoding")
	resp.Header.Del("Connection")
	resp.Body = io.NopCloser(&bodyBuf)
	resp.ContentLength = int64(bodyBuf.Len())
	if trailers != nil {
		resp.Trailer = trailers
	}
	resp.Request = req
	return resp, nil
}

func (t *cronetTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	addr := req.URL.Host
	if !strings.Contains(addr, ":") {
		addr += ":443"
	}
	c, err := t.connFor(req.Context(), addr)
	if err != nil {
		return nil, err
	}
	if c.closed.Load() {
		t.mu.Lock()
		delete(t.conns, addr)
		t.mu.Unlock()
		c, err = t.connFor(req.Context(), addr)
		if err != nil {
			return nil, err
		}
	}
	resp, err := c.roundTrip(req)
	if err != nil && c.closed.Load() {
		t.mu.Lock()
		if cur, ok := t.conns[addr]; ok && cur == c {
			delete(t.conns, addr)
		}
		t.mu.Unlock()
	}
	return resp, err
}

var _ http.RoundTripper = (*cronetTransport)(nil)
