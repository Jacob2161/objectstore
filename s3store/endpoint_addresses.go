package s3store

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"syscall"

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
		// The proxy resolves the name; a failure here is not a refusal.
		return nil
	}
	for _, ip := range resolved {
		if err := p.check(ip); err != nil {
			return fmt.Errorf("%s resolves to %w", hostname, err)
		}
	}
	return nil
}

// refuseRedirect makes every redirect the final response, which the SDK
// reports as an error. Every bucket request carries a signature, and a
// redirect would resend it, and the request body, wherever the endpoint
// points.
func refuseRedirect(*http.Request, []*http.Request) error {
	return http.ErrUseLastResponse
}

// newHTTPClient is the HTTP client of every bucket: the SDK's default
// transport, with the policy's control on its dialer and checkProxiedHost
// before a proxy, and no redirects. It is a plain http.Client because the
// SDK's own client follows 307 and 308 redirects and cannot be told not to.
func newHTTPClient(policy addressPolicy) *http.Client {
	transport := awshttp.NewBuildableClient().
		WithDialerOptions(func(dialer *net.Dialer) {
			dialer.Control = policy.control
		}).
		WithTransportOptions(func(transport *http.Transport) {
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
				return proxyURL, nil
			}
		}).
		GetTransport()
	return &http.Client{Transport: transport, CheckRedirect: refuseRedirect}
}
