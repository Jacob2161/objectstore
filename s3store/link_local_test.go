package s3store

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestLinkLocalEndpointsAreDenied checks that a bucket pointed at the cloud
// metadata address is refused before any request is sent, and that the
// dialer allows loopback and private addresses, for buckets on a LAN.
func TestLinkLocalEndpointsAreDenied(t *testing.T) {
	store, err := NewStore(&NewStoreArgs{
		Provider:        ProviderS3,
		Endpoint:        "http://169.254.169.254",
		Bucket:          "metadata",
		AccessKeyID:     "test",
		SecretAccessKey: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Get(t.Context(), "probe")
	if !errors.Is(err, ErrLinkLocalEndpoint) {
		t.Fatalf("a metadata endpoint: %v", err)
	}
	for _, address := range []string{"169.254.1.1:80", "[fe80::1]:443", "[fd00:ec2::254]:80"} {
		if err := denyLinkLocal("tcp", address, nil); !errors.Is(err, ErrLinkLocalEndpoint) {
			t.Errorf("%s: %v", address, err)
		}
	}
	for _, address := range []string{"127.0.0.1:9000", "10.0.0.5:9000", "[::1]:9000", "8.8.8.8:443"} {
		if err := denyLinkLocal("tcp", address, nil); err != nil {
			t.Errorf("%s was refused: %v", address, err)
		}
	}
	if !strings.Contains(ErrLinkLocalEndpoint.Error(), "link-local") {
		t.Fatal(ErrLinkLocalEndpoint)
	}
}

// TestProxiedLinkLocalHostsAreDenied checks the bucket host a proxy would
// be asked to reach: a link-local or metadata literal is refused, and a
// loopback or private one, or a name, passes to the proxy.
func TestProxiedLinkLocalHostsAreDenied(t *testing.T) {
	for _, endpoint := range []string{
		"http://169.254.169.254/latest/meta-data",
		"http://[fd00:ec2::254]/",
		"http://[fe80::1]:9000/bucket",
	} {
		req := httptest.NewRequest(http.MethodGet, endpoint, nil)
		if err := checkProxiedHost(req); !errors.Is(err, ErrLinkLocalEndpoint) {
			t.Errorf("%s through a proxy: %v", endpoint, err)
		}
	}
	for _, endpoint := range []string{
		"http://127.0.0.1:9000/bucket",
		"http://10.0.0.5:9000/bucket",
		"http://localhost:9000/bucket",
	} {
		req := httptest.NewRequest(http.MethodGet, endpoint, nil)
		if err := checkProxiedHost(req); err != nil {
			t.Errorf("%s through a proxy was refused: %v", endpoint, err)
		}
	}
}
