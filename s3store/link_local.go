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
// link-local address, where cloud metadata services answer.
var ErrLinkLocalEndpoint = errors.New(
	"bucket endpoints may not reach link-local addresses",
)

// awsMetadataAddress is the IPv6 address of the EC2 metadata service, which
// is not link-local.
var awsMetadataAddress = netip.MustParseAddr("fd00:ec2::254")

// deniedAddress reports an address no bucket connection may reach.
func deniedAddress(ip netip.Addr) bool {
	ip = ip.Unmap()
	return ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip == awsMetadataAddress
}

// denyLinkLocal is a dialer control that refuses a connection to a
// link-local address (169.254.0.0/16, fe80::/10) or the EC2 metadata
// address. It runs after name resolution, so a name that resolves there is
// refused too. Private and loopback addresses stay allowed, for buckets on
// a LAN.
func denyLinkLocal(_ string, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return err
	}
	if deniedAddress(ip) {
		return fmt.Errorf("%w: %s", ErrLinkLocalEndpoint, ip.Unmap())
	}
	return nil
}

// checkProxiedHost refuses a request a proxy would carry to a link-local
// address. Through a proxy the dialer sees only the proxy's address, so the
// bucket's own host is checked here, as a literal or as this host resolves
// it; a proxy that resolves the name differently is the proxy's to police.
func checkProxiedHost(req *http.Request) error {
	hostname := req.URL.Hostname()
	literal, err := netip.ParseAddr(hostname)
	if err == nil {
		if deniedAddress(literal) {
			return fmt.Errorf("%w: %s", ErrLinkLocalEndpoint, literal.Unmap())
		}
		return nil
	}
	resolved, err := net.DefaultResolver.LookupNetIP(req.Context(), "ip", hostname)
	if err != nil {
		// The proxy resolves the name; a failure here is not a refusal.
		return nil
	}
	for _, ip := range resolved {
		if deniedAddress(ip) {
			return fmt.Errorf("%w: %s resolves to %s", ErrLinkLocalEndpoint, hostname, ip.Unmap())
		}
	}
	return nil
}

// newHTTPClient is the HTTP client of every bucket: the SDK's default,
// with denyLinkLocal on its dialer and checkProxiedHost before a proxy.
func newHTTPClient() *awshttp.BuildableClient {
	return awshttp.NewBuildableClient().
		WithDialerOptions(func(dialer *net.Dialer) {
			dialer.Control = denyLinkLocal
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
				if err := checkProxiedHost(req); err != nil {
					return nil, err
				}
				return proxyURL, nil
			}
		})
}
