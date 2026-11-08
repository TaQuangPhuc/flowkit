// tlsrelay — one binary, two jobs:
//
//  1. Per-nick gRPC TLS relay: h2c listeners on 127.0.0.1:51900+i, each
//     re-originating to aisandbox-pa.googleapis.com:443 through the nick's
//     upstream proxy using a uTLS iOS ClientHello — so Python's grpc stack
//     never touches the wire with its own TLS fingerprint.
//  2. Zero-downtime public gateway on :8100 forwarding to the healthiest
//     backend instance (127.0.0.1:8101 / :8102). Deploy = start the idle
//     port, let health flip, drain the old one.
package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"crypto/tls"

	utls "github.com/refraction-networking/utls"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

const targetHost = "aisandbox-pa.googleapis.com"
const relayBase = 51900

var backendPorts = []int{8101, 8102}
var rootDir = "."
var gwTarget atomic.Value // int — preferred backend port, or 0

// ---------------------------------------------------------------------------
// graceful restart support
//
// Listeners are bound with SO_REUSEPORT so a temporary "bridge" instance can
// take over while the old process drains. On SIGTERM we Shutdown() every
// server: listeners close (new conns land on the reuseport partner), the
// gateway's h1 conns drain natively, and h2c conns get GOAWAY via the
// http.Server registered through ServeConnOpts.BaseConfig. Hijacked h2 conns
// are not waited on by Shutdown, so we poll the in-flight request counter and
// only then close any lingering sockets.
// ---------------------------------------------------------------------------

var (
	inflight  atomic.Int64
	listeners []*trackListener
	servers   []*http.Server
)

type trackListener struct {
	net.Listener
	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

func (l *trackListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	l.conns[c] = struct{}{}
	l.mu.Unlock()
	return &trackConn{Conn: c, parent: l}, nil
}

func (l *trackListener) closeConns() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for c := range l.conns {
		c.Close()
	}
}

type trackConn struct {
	net.Conn
	parent *trackListener
	once   sync.Once
}

func (c *trackConn) Close() error {
	var err error
	c.once.Do(func() {
		c.parent.mu.Lock()
		delete(c.parent.conns, c.Conn)
		c.parent.mu.Unlock()
		err = c.Conn.Close()
	})
	return err
}

func listenShared(addr string) (net.Listener, error) {
	lc := net.ListenConfig{Control: reusePortControl}
	l, err := lc.Listen(context.Background(), "tcp", addr)
	if err != nil {
		return nil, err
	}
	tl := &trackListener{Listener: l, conns: map[net.Conn]struct{}{}}
	listeners = append(listeners, tl)
	return tl, nil
}

// counted wraps the handler the h2c server dispatches per stream — inflight
// measures live gRPC calls, not whole connections.
func counted(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inflight.Add(1)
		defer inflight.Add(-1)
		h.ServeHTTP(w, r)
	})
}

func watchSignals() {
	ch := make(chan os.Signal, 2)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
	<-ch
	log.Printf("SIGTERM — draining (inflight=%d)", inflight.Load())
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for _, srv := range servers {
		srv := srv
		go func() {
			if err := srv.Shutdown(ctx); err != nil {
				log.Printf("shutdown: %v", err)
			}
		}()
	}
	// Shutdown sent GOAWAY to every h2c conn and closed the listeners; new
	// conns/streams now land on the reuseport partner. Wait for the streams
	// already in flight (video submits take a few seconds), then force-close
	// whatever is left.
	deadline := time.Now().Add(75 * time.Second)
	for inflight.Load() > 0 && time.Now().Before(deadline) {
		time.Sleep(250 * time.Millisecond)
	}
	for _, l := range listeners {
		l.closeConns()
	}
	log.Printf("drained, exiting (inflight=%d)", inflight.Load())
	os.Exit(0)
}

func loadTarget() {
	// zero_restart.py writes .scratch/gw_target with the port that should
	// serve traffic.
	b, err := os.ReadFile(filepath.Join(rootDir, ".scratch", "gw_target"))
	if err != nil {
		return
	}
	var p int
	fmt.Sscanf(strings.TrimSpace(string(b)), "%d", &p)
	if p > 0 {
		gwTarget.Store(p)
	}
}

// ---------------------------------------------------------------------------
// per-nick gRPC relay
// ---------------------------------------------------------------------------

func connectViaProxy(ctx context.Context, proxyURL, addr string) (net.Conn, error) {
	if proxyURL == "" {
		d := net.Dialer{Timeout: 12 * time.Second}
		return d.DialContext(ctx, "tcp", addr)
	}
	u, err := url.Parse(proxyURL)
	if err != nil {
		return nil, err
	}
	d := net.Dialer{Timeout: 12 * time.Second}
	raw, err := d.DialContext(ctx, "tcp", u.Host)
	if err != nil {
		return nil, err
	}
	auth := ""
	if u.User != nil {
		auth = "Proxy-Authorization: Basic " +
			base64.StdEncoding.EncodeToString(
				[]byte(u.User.String())) + "\r\n"
	}
	fmt.Fprintf(raw, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n%s\r\n",
		addr, addr, auth)
	br := bufio.NewReader(raw)
	status, err := br.ReadString('\n')
	if err != nil || !strings.Contains(status, "200") {
		raw.Close()
		return nil, fmt.Errorf("proxy CONNECT: %s", strings.TrimSpace(status))
	}
	for { // drain headers
		line, err := br.ReadString('\n')
		if err != nil || line == "\r\n" {
			break
		}
	}
	// leftover buffered bytes belong to TLS data — re-wrap reader+conn
	if br.Buffered() > 0 {
		return &bufConn{Conn: raw, r: br}, nil
	}
	return raw, nil
}

type bufConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *bufConn) Read(p []byte) (int, error) { return c.r.Read(p) }

// iosSpec returns the ClientHelloSpec matching the real Flow iOS app's
// SecureTransport hello, captured live from the friend iPhone on
// 2026-09-28 (iOS 18.x — X25519MLKEM768 hybrid key share, no GREASE).
// Only key-share key material differs per connection, as it should.
func iosSpec(alpn []string) *utls.ClientHelloSpec {
	return &utls.ClientHelloSpec{
		TLSVersMin: 0x0303,
		TLSVersMax: 0x0304,
		CipherSuites: []uint16{
			0x1301, 0x1302, 0x1303, // TLS1.3 AES-128/256 + CHACHA20
			0xc02b, 0xc02f, 0xc02c, 0xc030, // ECDHE ECDSA/RSA AES-GCM
			0xcca9, 0xcca8, // ECDHE CHACHA20
			0xc009, 0xc013, 0xc00a, 0xc014, // ECDHE AES-CBC
			0x9c, 0x9d, 0x2f, 0x35, // RSA AES-GCM/CBC fallback
		},
		CompressionMethods: []byte{0x00},
		Extensions: []utls.TLSExtension{
			&utls.SNIExtension{},
			&utls.ExtendedMasterSecretExtension{},
			&utls.RenegotiationInfoExtension{
				Renegotiation: utls.RenegotiateOnceAsClient},
			&utls.SupportedCurvesExtension{Curves: []utls.CurveID{
				utls.X25519MLKEM768, utls.X25519,
				utls.CurveP256, utls.CurveP384}},
			&utls.SupportedPointsExtension{SupportedPoints: []byte{0x00}},
			&utls.SessionTicketExtension{},
			&utls.ALPNExtension{AlpnProtocols: alpn},
			&utls.SignatureAlgorithmsExtension{
				SupportedSignatureAlgorithms: []utls.SignatureScheme{
					0x0403, 0x0804, 0x0401, 0x0503, 0x0805,
					0x0501, 0x0806, 0x0601, 0x0201}},
			&utls.KeyShareExtension{KeyShares: []utls.KeyShare{
				{Group: utls.X25519MLKEM768}, {Group: utls.X25519}}},
			&utls.PSKKeyExchangeModesExtension{Modes: []uint8{0x01}},
			&utls.SupportedVersionsExtension{
				Versions: []uint16{0x0304, 0x0303}},
		},
	}
}

func iosTLSConn(ctx context.Context, proxyURL, addr string) (net.Conn, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
		addr = host + ":443"
	}
	raw, err := connectViaProxy(ctx, proxyURL, addr)
	if err != nil {
		return nil, err
	}
	// the app's gRPC stack offers grpc-exp first; plain data calls offer h2
	alpn := []string{"h2"}
	if host == targetHost {
		alpn = []string{"grpc-exp", "h2"}
	}
	c := utls.UClient(raw, &utls.Config{ServerName: host}, utls.HelloCustom)
	if err := c.ApplyPreset(iosSpec(alpn)); err != nil {
		raw.Close()
		return nil, err
	}
	if err := c.HandshakeContext(ctx); err != nil {
		raw.Close()
		return nil, err
	}
	return c, nil
}

func newRelayTransport(proxyURL string) http.RoundTripper {
	dial := func(ctx context.Context, addr string) (net.Conn, error) {
		return iosTLSConn(ctx, proxyURL, addr)
	}
	if os.Getenv("H2_STD") == "1" {
		// fallback: stock Go h2 stack
		return &http2.Transport{
			AllowHTTP: false,
			DialTLSContext: func(ctx context.Context, network, addr string,
				_ *tls.Config) (net.Conn, error) {
				return dial(ctx, addr)
			},
		}
	}
	return &cronetTransport{dialTLS: dial}
}

// Only Google-owned destinations are allowed through the /to/ route —
// the relay is a fingerprint fixer, not an open proxy.
var allowedHostRe = regexp.MustCompile(
	`(?i)^([a-z0-9-]+\.)*(googleapis\.com|googleusercontent\.com|google\.com|recaptcha\.net|gstatic\.com|flow-content\.google)$`)

type ctxHost struct{}

// targetFor resolves the upstream host + rewritten path.
// h2 gRPC calls (no /to/ prefix) always go to aisandbox; h1 calls may
// address any allow-listed Google host via /to/<host>/<path…>.
func targetFor(r *http.Request) (host, path string, ok bool) {
	if strings.HasPrefix(r.URL.Path, "/to/") {
		rest := r.URL.Path[4:]
		h, p := rest, "/"
		if i := strings.IndexByte(rest, '/'); i >= 0 {
			h, p = rest[:i], rest[i:]
		}
		if !allowedHostRe.MatchString(h) {
			return "", "", false
		}
		return h, p, true
	}
	return targetHost, r.URL.Path, true
}

func relayHandler(tr http.RoundTripper) http.Handler {
	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			hp := pr.In.Context().Value(ctxHost{}).([2]string)
			pr.Out.URL.Scheme = "https"
			pr.Out.URL.Host = hp[0]
			pr.Out.URL.Path = hp[1]
			pr.Out.Host = hp[0]
		},
		Transport: tr,
		// streaming bodies + trailers matter for gRPC
		FlushInterval: -1,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, e error) {
			http.Error(w, "relay upstream error", 502)
			log.Printf("relay %s %s: %v", r.Method, r.URL.Path, e)
		},
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, path, ok := targetFor(r)
		if !ok {
			http.Error(w, "host not allowed", 403)
			return
		}
		ctx := context.WithValue(r.Context(), ctxHost{}, [2]string{host, path})
		rp.ServeHTTP(w, r.WithContext(ctx))
	})
}

var (
	relayMu      sync.Mutex
	relayPorts   = map[string]int{}
	relayProxies = map[string]string{}
	nextPortIdx  = 0
)

func hasNickCreds(dir string) bool {
	for _, f := range []string{"labs_cookie.txt", "refresh_token.txt", "aas_et.txt"} {
		if _, err := os.Stat(filepath.Join(dir, f)); err == nil {
			return true
		}
	}
	return false
}

func syncRelaysOnce(nicksDir string) {
	entries, err := os.ReadDir(nicksDir)
	if err != nil {
		return
	}
	activeSet := map[string]bool{}
	var nicks []string
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), "_") {
			continue
		}
		if hasNickCreds(filepath.Join(nicksDir, e.Name())) {
			nicks = append(nicks, e.Name())
			activeSet[e.Name()] = true
		}
	}
	sort.Strings(nicks)

	relayMu.Lock()
	defer relayMu.Unlock()
	changed := false
	for existing := range relayPorts {
		if !activeSet[existing] {
			delete(relayPorts, existing)
			delete(relayProxies, existing)
			changed = true
		}
	}
	for _, nick := range nicks {
		proxy := ""
		if b, err := os.ReadFile(filepath.Join(nicksDir, nick, "proxy.txt")); err == nil {
			proxy = strings.TrimSpace(string(b))
		}
		if _, exists := relayPorts[nick]; exists && relayProxies[nick] == proxy {
			continue
		}
		port := relayBase + nextPortIdx
		nextPortIdx++
		relayPorts[nick] = port
		relayProxies[nick] = proxy
		changed = true

		h := relayHandler(newRelayTransport(proxy))
		addr := fmt.Sprintf("127.0.0.1:%d", port)
		srv := &http.Server{Handler: h2c.NewHandler(counted(h), &http2.Server{})}
		servers = append(servers, srv)
		go func(addr, nick string, srv *http.Server, hasProxy bool) {
			l, err := listenShared(addr)
			if err != nil {
				log.Printf("relay %s listen error: %v", nick, err)
				return
			}
			log.Printf("relay %-8s h2c %s → %s (proxy=%v)", nick, addr, targetHost, hasProxy)
			srv.Serve(l)
		}(addr, nick, srv, proxy != "")
	}
	relayFile := filepath.Join(nicksDir, "_relay.json")
	if _, statErr := os.Stat(relayFile); statErr != nil {
		changed = true
	}
	if changed {
		b, _ := json.Marshal(relayPorts)
		tmp := relayFile + ".tmp"
		if err := os.WriteFile(tmp, b, 0o644); err == nil {
			_ = os.Rename(tmp, relayFile)
		}
	}
}

func startRelays(nicksDir string) {
	_ = os.MkdirAll(nicksDir, 0o700)
	if os.Getenv("H2_STD") == "1" {
		log.Printf("h2 transport: std x/net/http2 (H2_STD=1)")
	} else {
		log.Printf("h2 transport: h2cronet")
	}
	syncRelaysOnce(nicksDir)
	go func() {
		for {
			time.Sleep(3 * time.Second)
			syncRelaysOnce(nicksDir)
		}
	}()
}

// ---------------------------------------------------------------------------
// zero-downtime gateway
// ---------------------------------------------------------------------------

var healthy atomic.Value // map[int]bool

func probe(port int) bool {
	c := &http.Client{Timeout: 4 * time.Second}
	r, err := c.Get(fmt.Sprintf("http://127.0.0.1:%d/health", port))
	if err != nil {
		return false
	}
	defer r.Body.Close()
	io.Copy(io.Discard, r.Body)
	return r.StatusCode == 200
}

func healthLoop() {
	// Mark a backend down only after 3 consecutive probe failures (~4.5s+).
	// A busy agent can take >1.5s on /health under CPU saturation — one slow
	// probe used to flap the whole gateway into "no healthy backend" 502s
	// while the agent was still serving fine.
	streak := map[int]int{}
	m := map[int]bool{}
	for {
		for _, p := range backendPorts {
			if probe(p) {
				streak[p] = 0
				m[p] = true
			} else {
				streak[p]++
				m[p] = m[p] && streak[p] < 3
			}
		}
		healthy.Store(m)
		loadTarget()
		time.Sleep(1500 * time.Millisecond)
	}
}

var rrCounter atomic.Uint64

func pickBackend() int {
	m, _ := healthy.Load().(map[int]bool)
	if p, ok := gwTarget.Load().(int); ok && m[p] {
		return p // the deploy-designated backend while it stays healthy
	}
	// Round-robin across every healthy backend: both agents serve traffic
	// concurrently, halving the per-process asyncio loop load.
	var up []int
	for _, p := range backendPorts {
		if m[p] {
			up = append(up, p)
		}
	}
	if len(up) == 0 {
		return 0
	}
	return up[int(rrCounter.Add(1))%len(up)]
}

func startGateway(listen string) {
	tr := &http.Transport{
		MaxIdleConns:        256,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
		ForceAttemptHTTP2:   false,
	}
	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			port := pickBackend()
			pr.Out.URL.Scheme = "http"
			pr.Out.URL.Host = fmt.Sprintf("127.0.0.1:%d", port)
		},
		Transport:     tr,
		FlushInterval: -1,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, e error) {
			http.Error(w, `{"detail":"no healthy backend"}`, 502)
		},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/__gw/status", func(w http.ResponseWriter, r *http.Request) {
		m, _ := healthy.Load().(map[int]bool)
		json.NewEncoder(w).Encode(map[string]any{
			"healthy": m, "serving": pickBackend()})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if pickBackend() == 0 {
			http.Error(w, `{"detail":"no healthy backend"}`, 502)
			return
		}
		rp.ServeHTTP(w, r)
	})
	log.Printf("gateway %s → backends %v", listen, backendPorts)
	var l net.Listener
	var err error
	for i := 0; i < 30; i++ { // retry-bind while old backend releases the port
		l, err = listenShared(listen)
		if err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		log.Fatalf("gateway bind %s: %v", listen, err)
	}
	srv := &http.Server{Handler: counted(mux)}
	servers = append(servers, srv)
	if err := srv.Serve(l); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
	// graceful restart: Shutdown closed the listener — stay alive while
	// watchSignals finishes draining, it owns os.Exit
	select {}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	go watchSignals()
	root := "."
	if len(os.Args) > 1 {
		root = os.Args[1]
	}
	rootDir = root
	gwListen := os.Getenv("GW_LISTEN")
	if gwListen == "" {
		gwListen = "0.0.0.0:8100"
	}
	if os.Getenv("GW_ONLY") == "1" {
		go healthLoop()
		startGateway(gwListen)
		return
	}
	if os.Getenv("RELAY_ONLY") != "1" {
		go healthLoop()
	}
	startRelays(filepath.Join(root, "nicks"))
	if os.Getenv("RELAY_ONLY") == "1" {
		select {} // relay only — no gateway
	}
	startGateway(gwListen)
}
