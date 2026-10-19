// Package gy implements the 4G Gy/Ro Diameter Credit-Control interface
// (RFC 4006 with 3GPP TS 32.299 usage) as a server: it accepts peer
// connections, runs the base-protocol capabilities exchange and watchdog,
// decodes CCRs into canonical charging requests, calls the rating engine and
// encodes the CCA.
//
// Only TCP is supported here. The server takes any net.Listener, so an SCTP
// listener can be plugged in without changes.
package gy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/diameter"
	"github.com/cckhaw/meridian-ocs/pkg/engine"
	"github.com/cckhaw/meridian-ocs/services/ocs-signaling-gateway/internal/admission"
)

// Rater is the rating engine as seen by the adapter (*engine.Engine).
type Rater interface {
	Rate(ctx context.Context, req *canonical.Request, resp *canonical.Response) error
}

// Config configures the server. Zero values get defaults in NewServer except
// the identity fields, which are required.
type Config struct {
	OriginHost  string
	OriginRealm string
	HostIP      net.IP
	ProductName string
	VendorID    uint32

	MaxMessage         int           // largest accepted message, default 64 KiB
	MaxMSCC            int           // per CCR, default 16
	MaxInflightPerConn int           // concurrent CCRs per peer connection, default 256
	WatchdogInterval   time.Duration // Tw, default 30s; a silent peer is dropped after 3x
	WriteTimeout       time.Duration // default 5s

	// Amounts granted when a Requested-Service-Unit is present but empty.
	DefaultRequestedBytes   int64 // default 10 MiB
	DefaultRequestedSeconds int64 // default 300
	DefaultRequestedEvents  int64 // default 1
}

func (c *Config) setDefaults() error {
	if c.OriginHost == "" || c.OriginRealm == "" || c.HostIP == nil {
		return errors.New("gy: OriginHost, OriginRealm and HostIP are required")
	}
	if c.ProductName == "" {
		c.ProductName = "meridian-ocs"
	}
	if c.MaxMessage <= 0 {
		c.MaxMessage = 64 << 10
	}
	if c.MaxMSCC <= 0 {
		c.MaxMSCC = 16
	}
	if c.MaxInflightPerConn <= 0 {
		c.MaxInflightPerConn = 256
	}
	if c.WatchdogInterval <= 0 {
		c.WatchdogInterval = 30 * time.Second
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = 5 * time.Second
	}
	if c.DefaultRequestedBytes <= 0 {
		c.DefaultRequestedBytes = 10 << 20
	}
	if c.DefaultRequestedSeconds <= 0 {
		c.DefaultRequestedSeconds = 300
	}
	if c.DefaultRequestedEvents <= 0 {
		c.DefaultRequestedEvents = 1
	}
	return nil
}

// Stats are cumulative counters.
type Stats struct {
	CCRs       uint64
	Shed       uint64 // answered 3004 by admission control or engine overload
	Rejected   uint64 // malformed or unsupported requests
	PeersOpen  int64
	PeersTotal uint64
}

// Server is a Gy Diameter server.
type Server struct {
	cfg   Config
	rater Rater
	adm   *admission.Admission

	mu        sync.Mutex
	listeners map[net.Listener]struct{}
	conns     map[*peerConn]struct{}
	closing   bool
	wg        sync.WaitGroup

	ccrs, shed, rejected, peersTotal atomic.Uint64
	peersOpen                        atomic.Int64
	hbh                              atomic.Uint32
	e2eCounter                       atomic.Uint32
	e2eBoot                          uint32 // high 12 bits of End-to-End ids (RFC 6733 §3)
}

// NewServer builds a server. adm may be nil (no admission control).
func NewServer(cfg Config, rater Rater, adm *admission.Admission) (*Server, error) {
	if rater == nil {
		return nil, errors.New("gy: rater is required")
	}
	if err := cfg.setDefaults(); err != nil {
		return nil, err
	}
	if adm == nil {
		adm = admission.New(0, 0, 0)
	}
	s := &Server{cfg: cfg, rater: rater, adm: adm,
		listeners: map[net.Listener]struct{}{}, conns: map[*peerConn]struct{}{}}
	s.e2eBoot = uint32(time.Now().Unix()) << 20
	return s, nil
}

// Stats returns a snapshot of the counters.
func (s *Server) Stats() Stats {
	return Stats{s.ccrs.Load(), s.shed.Load(), s.rejected.Load(), s.peersOpen.Load(), s.peersTotal.Load()}
}

// Admission returns the admission controller (for runtime tuning).
func (s *Server) Admission() *admission.Admission { return s.adm }

// Serve accepts peers on ln until Close. It returns nil after Close.
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		_ = ln.Close()
		return nil
	}
	s.listeners[ln] = struct{}{}
	s.mu.Unlock()

	for {
		c, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			closing := s.closing
			s.mu.Unlock()
			if closing {
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			return err
		}
		pc := &peerConn{s: s, c: c, sem: make(chan struct{}, s.cfg.MaxInflightPerConn)}
		s.mu.Lock()
		if s.closing {
			s.mu.Unlock()
			_ = c.Close()
			return nil
		}
		s.conns[pc] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		go pc.serve()
	}
}

// Close stops accepting, closes all peers and waits for their handlers.
func (s *Server) Close() error {
	s.mu.Lock()
	s.closing = true
	for ln := range s.listeners {
		_ = ln.Close()
	}
	for pc := range s.conns {
		_ = pc.c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	return nil
}

func (s *Server) nextIDs() (hbh, e2e uint32) {
	return s.hbh.Add(1), s.e2eBoot | s.e2eCounter.Add(1)&0xFFFFF
}

var (
	bufPool     = sync.Pool{New: func() any { b := make([]byte, 0, 2048); return &b }}
	builderPool = sync.Pool{New: func() any { return &diameter.Builder{B: make([]byte, 0, 1024)} }}
)

func getBuf(n int) *[]byte {
	p := bufPool.Get().(*[]byte)
	if cap(*p) < n {
		*p = make([]byte, n)
	}
	*p = (*p)[:n]
	return p
}

type peerConn struct {
	s    *Server
	c    net.Conn
	wmu  sync.Mutex
	sem  chan struct{}
	open bool // CER/CEA completed; only touched by the read loop

	lastRead atomic.Int64 // unix nanos
	reqWG    sync.WaitGroup
	cancel   context.CancelFunc
}

func (pc *peerConn) serve() {
	s := pc.s
	s.peersOpen.Add(1)
	s.peersTotal.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	pc.cancel = cancel
	pc.lastRead.Store(time.Now().UnixNano())
	done := make(chan struct{})
	go pc.watchdog(done)
	defer func() {
		close(done)
		cancel()
		_ = pc.c.Close()
		pc.reqWG.Wait()
		s.mu.Lock()
		delete(s.conns, pc)
		s.mu.Unlock()
		s.peersOpen.Add(-1)
		s.wg.Done()
	}()

	var hdr [diameter.HeaderLen]byte
	for {
		_ = pc.c.SetReadDeadline(time.Now().Add(3 * s.cfg.WatchdogInterval))
		if _, err := io.ReadFull(pc.c, hdr[:]); err != nil {
			return
		}
		h, err := diameter.ParseHeader(hdr[:])
		if err != nil || int(h.Length) > s.cfg.MaxMessage {
			return
		}
		bp := getBuf(int(h.Length))
		copy(*bp, hdr[:])
		if _, err := io.ReadFull(pc.c, (*bp)[diameter.HeaderLen:]); err != nil {
			bufPool.Put(bp)
			return
		}
		pc.lastRead.Store(time.Now().UnixNano())

		keep := pc.dispatch(ctx, h, bp)
		if !keep {
			return
		}
	}
}

// dispatch handles one message. It returns false when the connection must
// be closed. It takes ownership of bp.
func (pc *peerConn) dispatch(ctx context.Context, h diameter.Header, bp *[]byte) bool {
	s := pc.s
	msg := *bp

	if !pc.open {
		defer bufPool.Put(bp)
		if h.Command != diameter.CmdCER || !h.IsRequest {
			return false // RFC 6733: nothing but CER before the peer is open
		}
		ok := pc.handleCER(h, msg)
		pc.open = ok
		return ok
	}

	if !h.IsRequest {
		bufPool.Put(bp) // DWA and friends: arrival already refreshed the timer
		return true
	}

	switch h.Command {
	case diameter.CmdCCR:
		select {
		case pc.sem <- struct{}{}:
		default:
			// Too many outstanding on this connection: shed rather than queue.
			s.shed.Add(1)
			pc.answerEarly(h, msg, diameter.ResultTooBusy, "peer connection overloaded")
			bufPool.Put(bp)
			return true
		}
		pc.reqWG.Add(1)
		go func() {
			defer func() { <-pc.sem; pc.reqWG.Done(); bufPool.Put(bp) }()
			pc.handleCCR(ctx, h, msg)
		}()
		return true
	case diameter.CmdDWR:
		defer bufPool.Put(bp)
		pc.reply(h, diameter.CmdDWR, diameter.ResultSuccess)
		return true
	case diameter.CmdDPR:
		defer bufPool.Put(bp)
		pc.reply(h, diameter.CmdDPR, diameter.ResultSuccess)
		return false
	default:
		defer bufPool.Put(bp)
		pc.reply(h, h.Command, diameter.ResultCommandUnsupported)
		return true
	}
}

func (pc *peerConn) write(msg []byte) {
	pc.wmu.Lock()
	_ = pc.c.SetWriteDeadline(time.Now().Add(pc.s.cfg.WriteTimeout))
	_, err := pc.c.Write(msg)
	pc.wmu.Unlock()
	if err != nil {
		_ = pc.c.Close()
	}
}

// reply sends a base-protocol answer (DWA, DPA, errors) with Result-Code and
// our identity.
func (pc *peerConn) reply(h diameter.Header, cmd, result uint32) {
	s := pc.s
	b := builderPool.Get().(*diameter.Builder)
	defer builderPool.Put(b)
	s.beginAnswer(b, h, cmd, result)
	b.Uint32(diameter.AVPResultCode, diameter.AVPFlagMandatory, result)
	s.originAVPs(b)
	if msg, err := b.Finish(); err == nil {
		pc.write(msg)
	}
}

// answerEarly answers a CCR without decoding it fully (load shedding): it
// pulls out only what the answer must echo.
func (pc *peerConn) answerEarly(h diameter.Header, msg []byte, result uint32, errMsg string) {
	s := pc.s
	var info ccrInfo
	info.h = h
	data := msg[diameter.HeaderLen:]
	for len(data) > 0 {
		a, rest, err := diameter.NextAVP(data)
		if err != nil {
			break
		}
		data = rest
		switch {
		case a.Flags&diameter.AVPFlagVendor != 0:
		case a.Code == diameter.AVPSessionID:
			info.sessionID = a.Data
		case a.Code == avpCCType:
			if v, err := a.Uint32(); err == nil {
				info.reqType, info.haveType = v, true
			}
		case a.Code == avpCCNumber:
			if v, err := a.Uint32(); err == nil {
				info.reqNo, info.haveNo = v, true
			}
		}
	}
	b := builderPool.Get().(*diameter.Builder)
	defer builderPool.Put(b)
	if out, err := s.encodeCCA(b, &info, nil, nil, result, errMsg); err == nil {
		pc.write(out)
	}
}

func (pc *peerConn) handleCCR(ctx context.Context, h diameter.Header, msg []byte) {
	s := pc.s
	s.ccrs.Add(1)

	if !s.adm.Acquire() {
		s.shed.Add(1)
		pc.answerEarly(h, msg, diameter.ResultTooBusy, "overloaded")
		return
	}
	defer s.adm.Release()

	req := canonical.AcquireRequest()
	resp := canonical.AcquireResponse()
	defer canonical.ReleaseRequest(req)
	defer canonical.ReleaseResponse(resp)
	b := builderPool.Get().(*diameter.Builder)
	defer builderPool.Put(b)

	var info ccrInfo
	if derr := decodeCCR(msg, h, &s.cfg, req, &info); derr != nil {
		s.rejected.Add(1)
		if out, err := s.encodeCCA(b, &info, req, nil, derr.Result, derr.Msg); err == nil {
			pc.write(out)
		}
		return
	}

	result := uint32(diameter.ResultSuccess)
	errMsg := ""
	switch err := s.rater.Rate(ctx, req, resp); {
	case err == nil:
		result = resultCode(resp.Result)
	case errors.Is(err, engine.ErrOverloaded):
		s.shed.Add(1)
		result, errMsg = diameter.ResultTooBusy, "overloaded"
	case errors.Is(err, engine.ErrInvalid):
		s.rejected.Add(1)
		result, errMsg = diameter.ResultInvalidAVPValue, "invalid request"
	default: // closed, cancelled, ...
		result, errMsg = diameter.ResultUnableToDeliver, "unavailable"
	}
	if out, err := s.encodeCCA(b, &info, req, resp, result, errMsg); err == nil {
		pc.write(out)
	}
}

// handleCER runs the capabilities exchange. It reports whether the peer is
// now open.
func (pc *peerConn) handleCER(h diameter.Header, msg []byte) bool {
	var haveHost, haveRealm, haveApp bool
	data := msg[diameter.HeaderLen:]
	for len(data) > 0 {
		a, rest, err := diameter.NextAVP(data)
		if err != nil {
			pc.cerAnswer(h, diameter.ResultInvalidAVPLength)
			return false
		}
		data = rest
		switch a.Code {
		case diameter.AVPOriginHost:
			haveHost = len(a.Data) > 0
		case diameter.AVPOriginRealm:
			haveRealm = len(a.Data) > 0
		case diameter.AVPAuthApplicationID:
			if v, err := a.Uint32(); err == nil && v == diameter.AppCreditControl {
				haveApp = true
			}
		case diameter.AVPVendorSpecificAppID:
			g := a.Data
			for len(g) > 0 {
				ia, irest, err := diameter.NextAVP(g)
				if err != nil {
					break
				}
				g = irest
				if ia.Code == diameter.AVPAuthApplicationID {
					if v, err := ia.Uint32(); err == nil && v == diameter.AppCreditControl {
						haveApp = true
					}
				}
			}
		}
	}
	switch {
	case !haveHost || !haveRealm:
		pc.cerAnswer(h, diameter.ResultMissingAVP)
		return false
	case !haveApp:
		pc.cerAnswer(h, diameter.ResultNoCommonApplication)
		return false
	}
	pc.cerAnswer(h, diameter.ResultSuccess)
	return true
}

func (pc *peerConn) cerAnswer(h diameter.Header, result uint32) {
	s := pc.s
	b := builderPool.Get().(*diameter.Builder)
	defer builderPool.Put(b)
	s.beginAnswer(b, h, diameter.CmdCER, result)
	b.Uint32(diameter.AVPResultCode, diameter.AVPFlagMandatory, result)
	s.originAVPs(b)
	b.Address(diameter.AVPHostIPAddress, diameter.AVPFlagMandatory, s.cfg.HostIP)
	b.Uint32(diameter.AVPVendorID, diameter.AVPFlagMandatory, s.cfg.VendorID)
	b.String(diameter.AVPProductName, 0, s.cfg.ProductName)
	if result == diameter.ResultSuccess {
		b.Uint32(diameter.AVPAuthApplicationID, diameter.AVPFlagMandatory, diameter.AppCreditControl)
		b.Uint32(diameter.AVPSupportedVendorID, diameter.AVPFlagMandatory, diameter.Vendor3GPP)
	}
	if msg, err := b.Finish(); err == nil {
		pc.write(msg)
	}
}

// watchdog sends a Device-Watchdog-Request when the peer has been silent for
// one interval; the read deadline drops it after three.
func (pc *peerConn) watchdog(done <-chan struct{}) {
	s := pc.s
	t := time.NewTicker(s.cfg.WatchdogInterval / 2)
	defer t.Stop()
	for {
		select {
		case <-done:
			return
		case <-t.C:
			if time.Since(time.Unix(0, pc.lastRead.Load())) < s.cfg.WatchdogInterval {
				continue
			}
			b := builderPool.Get().(*diameter.Builder)
			hbh, e2e := s.nextIDs()
			b.StartMessage(diameter.FlagRequest, diameter.CmdDWR, diameter.AppBase, hbh, e2e)
			s.originAVPs(b)
			if msg, err := b.Finish(); err == nil {
				pc.write(msg)
			}
			builderPool.Put(b)
		}
	}
}

// String describes the server for logs.
func (s *Server) String() string {
	return fmt.Sprintf("gy.Server{%s@%s}", s.cfg.OriginHost, s.cfg.OriginRealm)
}
