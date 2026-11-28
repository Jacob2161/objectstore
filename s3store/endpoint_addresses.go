package s3store

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"sync"
	"syscall"
	"time"

	// Aliased: net/http shares the package name.
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
)

// ErrLinkLocalEndpoint reports a bucket endpoint that resolved to a
// link-local address, where cloud metadata services answer. It is refused
// whatever NewStoreArgs says.
var ErrLinkLocalEndpoint = errors.New(
	"bucket endpoints may not reach link-local addresses",
)

// ErrPrivateEndpoint reports a bucket endpoint that resolved to a private,
// loopback, or unspecified address of a store whose
// NewStoreArgs.PrivateAddressesAllowed is false.
var ErrPrivateEndpoint = errors.New(
	"bucket endpoints may not reach private or loopback addresses " +
		"unless PrivateAddressesAllowed is set",
)

// awsMetadataAddress is the IPv6 address of the EC2 metadata service, which
// is not link-local.
var awsMetadataAddress = netip.MustParseAddr("fd00:ec2::254")

// addressPolicy decides which addresses a bucket's connections may reach.
type addressPolicy struct {
	// privateAllowed lets connections reach private, loopback, and
	// unspecified addresses, for a bucket on a LAN or on this host.
	privateAllowed bool
}

// check refuses a link-local address (169.254.0.0/16, fe80::/10) or the EC2
// metadata address always, and a private (10/8, 172.16/12, 192.168/16,
// fc00::/7), loopback, or unspecified one unless the policy allows them.
func (p addressPolicy) check(ip netip.Addr) error {
	ip = ip.Unmap()
	switch {
	case ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip == awsMetadataAddress:
		return fmt.Errorf("%w: %s", ErrLinkLocalEndpoint, ip)
	case !p.privateAllowed && (ip.IsPrivate() || ip.IsLoopback() || ip.IsUnspecified()):
		return fmt.Errorf("%w: %s", ErrPrivateEndpoint, ip)
	}
	return nil
}

// control is a dialer control that refuses a connection the policy does not
// allow. It runs after name resolution, on the address being dialed, so a
// name that resolves to a refused address is refused too, however it
// resolves next time.
func (p addressPolicy) control(_ string, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return err
	}
	return p.check(ip)
}

// checkProxiedHost refuses a request a proxy would carry to an address the
// policy does not allow. Through a proxy the dialer sees only the proxy's
// address, so the bucket's own host is checked here, as a literal or as this
// host resolves it; a proxy that resolves the name differently is the
// proxy's to police.
func (p addressPolicy) checkProxiedHost(req *http.Request) error {
	hostname := req.URL.Hostname()
	literal, err := netip.ParseAddr(hostname)
	if err == nil {
		return p.check(literal)
	}
	resolved, err := net.DefaultResolver.LookupNetIP(req.Context(), "ip", hostname)
	if err != nil {
		// A name that does not resolve here cannot be checked, so it is
		// denied rather than left to whatever the proxy resolves it to.
		return fmt.Errorf("resolve %s to check its address: %w", hostname, err)
	}
	for _, ip := range resolved {
		if err := p.check(ip); err != nil {
			return fmt.Errorf("%s resolves to %w", hostname, err)
		}
	}
	return nil
}

// denyRedirect makes every redirect the final response, which the SDK
// reports as an error. Every bucket request carries a signature, and a
// redirect would resend it, and the request body, wherever the endpoint
// points.
func denyRedirect(*http.Request, []*http.Request) error {
	return http.ErrUseLastResponse
}

// dialTimeout and dialKeepAlive are the SDK's own dialer settings.
const (
	dialTimeout   = 30 * time.Second
	dialKeepAlive = 30 * time.Second
)

// proxyAddresses are the host:port addresses of the proxies requests were
// sent through. A connection to one is the proxy's, not the bucket's, so
// the address policy does not apply to it; checkProxiedHost checks the
// bucket the proxy carries the request to instead.
type proxyAddresses struct {
	addresses sync.Map
}

// add records a proxy's address as the transport dials it.
func (p *proxyAddresses) add(proxyURL *url.URL) {
	p.addresses.Store(proxyDialAddress(proxyURL), true)
}

// has reports whether address is a recorded proxy's.
func (p *proxyAddresses) has(address string) bool {
	_, found := p.addresses.Load(address)
	return found
}

// proxyDialAddress is the host:port the transport dials for a proxy, with
// the scheme's default port when the URL names none.
func proxyDialAddress(proxyURL *url.URL) string {
	port := proxyURL.Port()
	if port == "" {
		switch proxyURL.Scheme {
		case "https":
			port = "443"
		case "socks5", "socks5h":
			port = "1080"
		default:
			port = "80"
		}
	}
	return net.JoinHostPort(proxyURL.Hostname(), port)
}

// dialer connects to a recorded proxy as it is, and to any other address
// only through the policy's control.
func (p addressPolicy) dialer(proxies *proxyAddresses) func(
	ctx context.Context,
	network string,
	address string,
) (net.Conn, error) {
	direct := &net.Dialer{Timeout: dialTimeout, KeepAlive: dialKeepAlive, Control: p.control}
	toProxy := &net.Dialer{Timeout: dialTimeout, KeepAlive: dialKeepAlive}
	return func(ctx context.Context, network string, address string) (net.Conn, error) {
		if proxies.has(address) {
			return toProxy.DialContext(ctx, network, address)
		}
		return direct.DialContext(ctx, network, address)
	}
}

// newHTTPClient is the HTTP client of every bucket: the SDK's default
// transport, dialing through the policy (except to a proxy) and checking
// the bucket's host before a proxy, and no redirects. It is a plain http.Client because the
// SDK's own client follows 307 and 308 redirects and cannot be told not to.
func newHTTPClient(policy addressPolicy) *http.Client {
	proxies := &proxyAddresses{}
	transport := awshttp.NewBuildableClient().
		WithTransportOptions(func(transport *http.Transport) {
			transport.DialContext = policy.dialer(proxies)
			proxy := transport.Proxy
			if proxy == nil {
				return
			}
			transport.Proxy = func(req *http.Request) (*url.URL, error) {
				proxyURL, err := proxy(req)
				if err != nil || proxyURL == nil {
					return proxyURL, err
				}
				if err := policy.checkProxiedHost(req); err != nil {
					return nil, err
				}
				proxies.add(proxyURL)
				return proxyURL, nil
			}
		}).
		GetTransport()
	return &http.Client{Transport: transport, CheckRedirect: denyRedirect}
}
