package main

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

func dialerFor(addr string) func(context.Context, string) (net.Conn, error) {
	return func(ctx context.Context, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", addr)
	}
}

func newStdH2(t *testing.T, h http.Handler) *cronetTransport {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	h2s := &http2.Server{}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go h2s.ServeConn(c, &http2.ServeConnOpts{Handler: h})
		}
	}()
	return &cronetTransport{dialTLS: dialerFor(ln.Addr().String())}
}

func echoHandler(w http.ResponseWriter, r *http.Request) {
	b, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.Header().Set("Trailer", "Grpc-Status")
	w.Header().Set("Content-Type", "application/grpc")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(b)
	w.Header().Set("Grpc-Status", "0")
}

func do(t *testing.T, tr *cronetTransport, method, rawURL string, body []byte) *http.Response {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, rawURL, rdr)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/grpc")
	req.Header.Set("Te", "trailers")
	resp, err := tr.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestEmptyPost(t *testing.T) {
	tr := newStdH2(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	resp := do(t, tr, http.MethodPost, "https://example.com/empty", nil)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

func TestSmallEchoAndTrailers(t *testing.T) {
	tr := newStdH2(t, http.HandlerFunc(echoHandler))
	resp := do(t, tr, http.MethodPost, "https://example.com/v1/echo", []byte("hello"))
	b, _ := io.ReadAll(resp.Body)
	if string(b) != "hello" {
		t.Fatalf("body %q", b)
	}
	if got := resp.Trailer.Get("Grpc-Status"); got != "0" {
		t.Fatalf("grpc-status %q", got)
	}
}

func TestLargeBodyRespectsFlowControl(t *testing.T) {
	body := bytes.Repeat([]byte("x"), 80*1024)
	tr := newStdH2(t, http.HandlerFunc(echoHandler))
	resp := do(t, tr, http.MethodPost, "https://example.com/v1/echo", body)
	b, _ := io.ReadAll(resp.Body)
	if !bytes.Equal(b, body) {
		t.Fatalf("len got %d want %d", len(b), len(body))
	}
	if resp.Trailer.Get("Grpc-Status") != "0" {
		t.Fatalf("missing trailer")
	}
}

// The old chan(256) dropped events once a slow reader fell behind, and the
// stream then hung until the 120s deadline with no error.
func TestStreamQueueNeverDrops(t *testing.T) {
	s := newStream(1)
	for i := 0; i < 5000; i++ {
		s.push(h2ev{kind: 'D', f: h2Frame{payload: []byte{byte(i)}}})
	}
	select {
	case <-s.notify:
	default:
		t.Fatal("no notify")
	}
	if got := len(s.drain()); got != 5000 || s.failed.Load() {
		t.Fatalf("drained %d failed=%v", got, s.failed.Load())
	}
}

func TestManyDataFramesOneStream(t *testing.T) {
	const n = 3000
	tr := newStdH2(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Trailer", "Grpc-Status")
		w.WriteHeader(http.StatusOK)
		f := w.(http.Flusher)
		for i := 0; i < n; i++ {
			_, _ = w.Write([]byte("ab"))
			f.Flush()
		}
		w.Header().Set("Grpc-Status", "0")
	}))
	resp := do(t, tr, http.MethodPost, "https://example.com/v1/many", []byte("x"))
	b, _ := io.ReadAll(resp.Body)
	if len(b) != 2*n || resp.Trailer.Get("Grpc-Status") != "0" {
		t.Fatalf("len %d trailer %q", len(b), resp.Trailer.Get("Grpc-Status"))
	}
}

func TestSequentialReuse(t *testing.T) {
	tr := newStdH2(t, http.HandlerFunc(echoHandler))
	for i := 0; i < 40; i++ {
		payload := []byte(strings.Repeat("abc", i+1))
		resp := do(t, tr, http.MethodPost, "https://example.com/v1/echo", payload)
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if !bytes.Equal(b, payload) {
			t.Fatalf("req %d body mismatch %d vs %d", i, len(b), len(payload))
		}
		if resp.Trailer.Get("Grpc-Status") != "0" {
			t.Fatalf("req %d missing trailer", i)
		}
	}
}

func TestConcurrentStreams(t *testing.T) {
	tr := newStdH2(t, http.HandlerFunc(echoHandler))
	errc := make(chan error, 8)
	for i := 0; i < 8; i++ {
		go func(i int) {
			payload := bytes.Repeat([]byte{byte(i)}, 20000)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://example.com/v1/echo", bytes.NewReader(payload))
			req.Header.Set("Content-Type", "application/grpc")
			resp, err := tr.RoundTrip(req)
			if err != nil {
				errc <- err
				return
			}
			defer resp.Body.Close()
			b, _ := io.ReadAll(resp.Body)
			if !bytes.Equal(b, payload) || resp.Trailer.Get("Grpc-Status") != "0" {
				errc <- io.ErrUnexpectedEOF
				return
			}
			errc <- nil
		}(i)
	}
	for i := 0; i < 8; i++ {
		if err := <-errc; err != nil {
			t.Fatal(err)
		}
	}
}

func TestConnDrop(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		buf := make([]byte, 64)
		_, _ = c.Read(buf)
		c.Close()
	}()
	tr := &cronetTransport{dialTLS: dialerFor(ln.Addr().String())}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://example.com/x", strings.NewReader("hi"))
	_, err = tr.RoundTrip(req)
	if err == nil {
		t.Fatal("expected error on dropped conn")
	}
}

// TestInitialWindowThenTrailers is the failure Devin hit: a body larger
// than the spec's 65535 send window must wait for WINDOW_UPDATE, and the
// grpc-status trailer must still be delivered when the server follows the
// response with RST_STREAM NO_ERROR.
func TestInitialWindowThenTrailers(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	want := bytes.Repeat([]byte("y"), 80*1024)
	gotBody := make(chan []byte, 1)
	violated := make(chan struct{}, 1)
	go serveStrict(ln, want, gotBody, violated)

	tr := &cronetTransport{dialTLS: dialerFor(ln.Addr().String())}
	resp := do(t, tr, http.MethodPost, "https://example.com/v1/echo", want)
	b, _ := io.ReadAll(resp.Body)
	if string(b) != "OK" {
		t.Fatalf("resp body %q", b)
	}
	if resp.Trailer.Get("Grpc-Status") != "0" {
		t.Fatalf("trailer %q header %v", resp.Trailer.Get("Grpc-Status"), resp.Trailer)
	}
	select {
	case <-violated:
		t.Fatal("client sent DATA past the initial 65535 window")
	default:
	}
	select {
	case g := <-gotBody:
		if !bytes.Equal(g, want) {
			t.Fatalf("server got %d bytes", len(g))
		}
	case <-time.After(time.Second):
		t.Fatal("server did not finish reading the body")
	}
}

func serveStrict(ln net.Listener, want []byte, gotBody chan []byte, violated chan struct{}) {
	c, err := ln.Accept()
	if err != nil {
		return
	}
	defer c.Close()
	pre := make([]byte, len("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"))
	if _, err := io.ReadFull(c, pre); err != nil {
		return
	}
	// client SETTINGS + conn WINDOW_UPDATE
	if _, err = readFrame(c); err != nil {
		return
	}
	if _, err = readFrame(c); err != nil {
		return
	}
	_ = writeFrame(c, fSettings, 0, 0, nil)
	_ = writeFrame(c, fSettings, flagAck, 0, nil)

	hdr, err := readFrame(c)
	if err != nil || hdr.typ != fHeaders {
		return
	}
	sid := hdr.stream
	var got []byte
	consumed := 0
	end := hdr.flags&flagEndStream != 0
	for !end && consumed < 65535 {
		f, err := readFrame(c)
		if err != nil {
			return
		}
		if f.typ != fData {
			continue
		}
		got = append(got, f.payload...)
		consumed += f.flowLen
		if f.flags&flagEndStream != 0 {
			end = true
		}
	}
	c.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	extra, err := readFrame(c)
	c.SetReadDeadline(time.Time{})
	if err == nil && extra.typ == fData {
		violated <- struct{}{}
		return
	}
	inc := []byte{0x00, 0x0f, 0xff, 0xff} // 1_048_575
	_ = writeFrame(c, fWindow, 0, 0, inc)
	_ = writeFrame(c, fWindow, 0, sid, inc)
	deadline := time.Now().Add(3 * time.Second)
	for !end {
		c.SetReadDeadline(deadline)
		f, err := readFrame(c)
		if err != nil {
			return
		}
		if f.typ != fData {
			continue
		}
		got = append(got, f.payload...)
		if f.flags&flagEndStream != 0 {
			end = true
		}
	}
	gotBody <- got

	var buf bytes.Buffer
	enc := hpack.NewEncoder(&buf)
	_ = enc.WriteField(hpack.HeaderField{Name: ":status", Value: "200"})
	_ = enc.WriteField(hpack.HeaderField{Name: "content-type", Value: "application/grpc"})
	_ = writeFrame(c, fHeaders, flagEndHeaders, sid, buf.Bytes())
	_ = writeFrame(c, fData, 0, sid, []byte("OK"))
	buf.Reset()
	_ = enc.WriteField(hpack.HeaderField{Name: "grpc-status", Value: "0"})
	_ = writeFrame(c, fHeaders, flagEndHeaders|flagEndStream, sid, buf.Bytes())
	_ = writeFrame(c, fRst, 0, sid, []byte{0, 0, 0, 0})
	time.Sleep(200 * time.Millisecond)
}
