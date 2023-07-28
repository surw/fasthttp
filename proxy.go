package fasthttp

import (
	"context"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

func FastHttpDialer() DialFunc {
	return fasthttpHTTPDialerTimeout(0)
}
func fasthttpHTTPDialerTimeout(timeout time.Duration) DialFunc {
	return func(addr string) (net.Conn, error) {
		var conn net.Conn
		var err error
		if timeout == 0 {
			conn, err = Dial(addr)
		} else {
			conn, err = DialTimeout(addr, timeout)
		}
		if err != nil {
			return nil, err
		}
		return conn, nil
	}
}

func ProxyDial(addr string) (net.Conn, error) {
	return defaultDialer.Dial(addr)
}
func ProxyDialTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	return defaultDialer.DialTimeout(addr, timeout)
}
func ProxyDialDualStackTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	return defaultDialer.DialDualStackTimeout(addr, timeout)
}

var defaultProxyDialer = &LBDialer{}

// LBDialer contains options to control a group of Dial calls.
type LBDialer struct {
	// Concurrency controls the maximum number of concurrent Dials
	// that can be performed using this object.
	// Setting this to 0 means unlimited.
	//
	// WARNING: This can only be changed before the first Dial.
	// Changes made after the first Dial will not affect anything.
	Concurrency int

	// LocalAddr is the local address to use when dialing an
	// address.
	// If nil, a local address is automatically chosen.
	LocalAddr *net.TCPAddr

	// This may be used to override DNS resolving policy, like this:
	// var dialer = &LBDialer{
	// 	Resolver: &net.Resolver{
	// 		PreferGo:     true,
	// 		StrictErrors: false,
	// 		Dial: func (ctx context.Context, network, address string) (net.Conn, error) {
	// 			d := net.Dialer{}
	// 			return d.DialContext(ctx, "udp", "8.8.8.8:53")
	// 		},
	// 	},
	// }
	Resolver Resolver

	// DNSCacheDuration may be used to override the default DNS cache duration (DefaultDNSCacheDuration)
	DNSCacheDuration time.Duration

	tcpAddrsMap sync.Map

	concurrencyCh chan struct{}

	once sync.Once
}

// Dial dials the given TCP addr using tcp4.
//
// This function has the following additional features comparing to net.Dial:
//
//   - It reduces load on DNS resolver by caching resolved TCP addressed
//     for DNSCacheDuration.
//   - It dials all the resolved TCP addresses in round-robin manner until
//     connection is established. This may be useful if certain addresses
//     are temporarily unreachable.
//   - It returns ErrDialTimeout if connection cannot be established during
//     DefaultDialTimeout seconds. Use DialTimeout for customizing dial timeout.
//
// This dialer is intended for custom code wrapping before passing
// to Client.Dial or HostClient.Dial.
//
// For instance, per-host counters and/or limits may be implemented
// by such wrappers.
//
// The addr passed to the function must contain port. Example addr values:
//
//   - foobar.baz:443
//   - foo.bar:80
//   - aaa.com:8080
func (d *LBDialer) Dial(addr string) (net.Conn, error) {
	return d.dial(addr, false, DefaultDialTimeout)
}

// DialTimeout dials the given TCP addr using tcp4 using the given timeout.
//
// This function has the following additional features comparing to net.Dial:
//
//   - It reduces load on DNS resolver by caching resolved TCP addressed
//     for DNSCacheDuration.
//   - It dials all the resolved TCP addresses in round-robin manner until
//     connection is established. This may be useful if certain addresses
//     are temporarily unreachable.
//
// This dialer is intended for custom code wrapping before passing
// to Client.Dial or HostClient.Dial.
//
// For instance, per-host counters and/or limits may be implemented
// by such wrappers.
//
// The addr passed to the function must contain port. Example addr values:
//
//   - foobar.baz:443
//   - foo.bar:80
//   - aaa.com:8080
func (d *LBDialer) DialTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	return d.dial(addr, false, timeout)
}

// DialDualStack dials the given TCP addr using both tcp4 and tcp6.
//
// This function has the following additional features comparing to net.Dial:
//
//   - It reduces load on DNS resolver by caching resolved TCP addressed
//     for DNSCacheDuration.
//   - It dials all the resolved TCP addresses in round-robin manner until
//     connection is established. This may be useful if certain addresses
//     are temporarily unreachable.
//   - It returns ErrDialTimeout if connection cannot be established during
//     DefaultDialTimeout seconds. Use DialDualStackTimeout for custom dial
//     timeout.
//
// This dialer is intended for custom code wrapping before passing
// to Client.Dial or HostClient.Dial.
//
// For instance, per-host counters and/or limits may be implemented
// by such wrappers.
//
// The addr passed to the function must contain port. Example addr values:
//
//   - foobar.baz:443
//   - foo.bar:80
//   - aaa.com:8080
func (d *LBDialer) DialDualStack(addr string) (net.Conn, error) {
	return d.dial(addr, true, DefaultDialTimeout)
}

// DialDualStackTimeout dials the given TCP addr using both tcp4 and tcp6
// using the given timeout.
//
// This function has the following additional features comparing to net.Dial:
//
//   - It reduces load on DNS resolver by caching resolved TCP addressed
//     for DNSCacheDuration.
//   - It dials all the resolved TCP addresses in round-robin manner until
//     connection is established. This may be useful if certain addresses
//     are temporarily unreachable.
//
// This dialer is intended for custom code wrapping before passing
// to Client.Dial or HostClient.Dial.
//
// For instance, per-host counters and/or limits may be implemented
// by such wrappers.
//
// The addr passed to the function must contain port. Example addr values:
//
//   - foobar.baz:443
//   - foo.bar:80
//   - aaa.com:8080
func (d *LBDialer) DialDualStackTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	return d.dial(addr, true, timeout)
}

func (d *LBDialer) dial(addr string, dualStack bool, timeout time.Duration) (net.Conn, error) {
	d.once.Do(func() {
		if d.Concurrency > 0 {
			d.concurrencyCh = make(chan struct{}, d.Concurrency)
		}

		if d.DNSCacheDuration == 0 {
			d.DNSCacheDuration = DefaultDNSCacheDuration
		}

		go d.tcpAddrsClean()
	})

	addrs, idx, err := d.getTCPAddrs(addr, dualStack)
	if err != nil {
		return nil, err
	}
	network := "tcp4"
	if dualStack {
		network = "tcp"
	}

	var conn net.Conn
	n := uint32(len(addrs))
	deadline := time.Now().Add(timeout)
	for n > 0 {
		conn, err = d.tryDial(network, &addrs[idx%n], deadline, d.concurrencyCh)
		if err == nil {
			return conn, nil
		}
		if err == ErrDialTimeout {
			return nil, err
		}
		idx++
		n--
	}
	return nil, err
}

func (d *LBDialer) tryDial(network string, addr *net.TCPAddr, deadline time.Time, concurrencyCh chan struct{}) (net.Conn, error) {
	timeout := -time.Since(deadline)
	if timeout <= 0 {
		return nil, ErrDialTimeout
	}

	if concurrencyCh != nil {
		select {
		case concurrencyCh <- struct{}{}:
		default:
			tc := AcquireTimer(timeout)
			isTimeout := false
			select {
			case concurrencyCh <- struct{}{}:
			case <-tc.C:
				isTimeout = true
			}
			ReleaseTimer(tc)
			if isTimeout {
				return nil, ErrDialTimeout
			}
		}
		defer func() { <-concurrencyCh }()
	}

	dialer := net.Dialer{}
	if d.LocalAddr != nil {
		dialer.LocalAddr = d.LocalAddr
	}

	ctx, cancel_ctx := context.WithDeadline(context.Background(), deadline)
	defer cancel_ctx()
	conn, err := dialer.DialContext(ctx, network, addr.String())
	if err != nil && ctx.Err() == context.DeadlineExceeded {
		return nil, ErrDialTimeout
	}
	return conn, err
}

func (d *LBDialer) tcpAddrsClean() {
	expireDuration := 2 * d.DNSCacheDuration
	for {
		time.Sleep(time.Second)
		t := time.Now()
		d.tcpAddrsMap.Range(func(k, v interface{}) bool {
			if e, ok := v.(*tcpAddrEntry); ok && t.Sub(e.resolveTime) > expireDuration {
				d.tcpAddrsMap.Delete(k)
			}
			return true
		})
	}
}

func (d *LBDialer) getTCPAddrs(addr string, dualStack bool) ([]net.TCPAddr, uint32, error) {
	item, exist := d.tcpAddrsMap.Load(addr)
	e, ok := item.(*tcpAddrEntry)
	if exist && ok && e != nil && time.Since(e.resolveTime) > d.DNSCacheDuration {
		// Only let one goroutine re-resolve at a time.
		if atomic.SwapInt32(&e.pending, 1) == 0 {
			e = nil
		}
	}

	if e == nil {
		addrs, err := resolveTCPAddrs(addr, dualStack, d.Resolver)
		if err != nil {
			item, exist := d.tcpAddrsMap.Load(addr)
			e, ok = item.(*tcpAddrEntry)
			if exist && ok && e != nil {
				// Set pending to 0 so another goroutine can retry.
				atomic.StoreInt32(&e.pending, 0)
			}
			return nil, 0, err
		}

		e = &tcpAddrEntry{
			addrs:       randomOrderTCPAddr(addrs),
			resolveTime: time.Now(),
		}
		d.tcpAddrsMap.Store(addr, e)
	}

	idx := atomic.AddUint32(&e.addrsIdx, 1)
	return e.addrs, idx, nil
}

var random = rand.New(rand.NewSource(time.Now().Unix()))

func randomOrderTCPAddr(addrs []net.TCPAddr) []net.TCPAddr {
	result := make([]net.TCPAddr, len(addrs))
	newOrder := random.Perm(len(addrs))
	for i, idx := range newOrder {
		result[i] = addrs[idx]
	}
	return result
}
