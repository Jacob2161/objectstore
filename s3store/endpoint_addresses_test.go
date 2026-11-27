package s3store

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// TestLinkLocalEndpointsAreDenied checks that a bucket pointed at the cloud
// metadata address is refused before any request is sent, even when private
// addresses are allowed, and that the dialer refuses every link-local and
// metadata address.
func TestLinkLocalEndpointsAreDenied(t *testing.T) {
	store, err := NewStore(&NewStoreArgs{
		Provider:                ProviderS3,
		Endpoint:                "http://169.254.169.254",
		Bucket:                  "metadata",
		AccessKeyID:             "test",
		SecretAccessKey:         "test",
		PrivateAddressesAllowed: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Get(t.Context(), "probe")
	if !errors.Is(err, ErrLinkLocalEndpoint) {
		t.Fatalf("Get from a metadata endpoint = %v, want ErrLinkLocalEndpoint", err)
	}
	for _, policy := range []addressPolicy{{privateAllowed: false}, {privateAllowed: true}} {
		for _, address := range []string{"169.254.1.1:80", "[fe80::1]:443", "[fd00:ec2::254]:80"} {
			err := policy.control("tcp", address, nil)
			if !errors.Is(err, ErrLinkLocalEndpoint) {
				t.Errorf("%+v dialing %s = %v, want ErrLinkLocalEndpoint", policy, address, err)
			}
		}
	}
	if !strings.Contains(ErrLinkLocalEndpoint.Error(), "link-local") {
		t.Fatalf("ErrLinkLocalEndpoint = %q, want it to say link-local", ErrLinkLocalEndpoint)
	}
}

// TestPrivateEndpointsNeedConsent checks that private, loopback, and
// unspecified addresses are refused by default and allowed with
// PrivateAddressesAllowed, and that public addresses are allowed either way.
func TestPrivateEndpointsNeedConsent(t *testing.T) {
	private := []string{
		"127.0.0.1:9000",
		"[::1]:9000",
		"10.0.0.5:9000",
		"172.16.0.1:443",
		"192.168.1.10:9000",
		"[fd12:3456::1]:9000",
		"0.0.0.0:9000",
		"[::ffff:127.0.0.1]:9000",
	}
	for _, address := range private {
		err := addressPolicy{}.control("tcp", address, nil)
		if !errors.Is(err, ErrPrivateEndpoint) {
			t.Errorf("dialing %s by default = %v, want ErrPrivateEndpoint", address, err)
		}
		if err := (addressPolicy{privateAllowed: true}).control("tcp", address, nil); err != nil {
			t.Errorf("dialing %s with private addresses allowed = %v, want nil", address, err)
		}
	}
	for _, address := range []string{"8.8.8.8:443", "[2001:4860:4860::8888]:443"} {
		if err := (addressPolicy{}).control("tcp", address, nil); err != nil {
			t.Errorf("dialing public %s = %v, want nil", address, err)
		}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		t.Errorf("a refused endpoint was sent %s %s", req.Method, req.URL)
	}))
	t.Cleanup(server.Close)
	store, err := NewStore(&NewStoreArgs{
		Provider:        ProviderS3,
		Endpoint:        server.URL,
		Bucket:          "local",
		AccessKeyID:     "test",
		SecretAccessKey: "test",
		PathStyle:       true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(t.Context(), "probe"); !errors.Is(err, ErrPrivateEndpoint) {
		t.Fatalf("Get from a loopback endpoint by default = %v, want ErrPrivateEndpoint", err)
	}
}

// TestProxiedHostsAreChecked checks the bucket host a proxy would be asked
// to reach: a link-local or metadata literal is refused always, a loopback
// or private one unless allowed, and a public one never.
func TestProxiedHostsAreChecked(t *testing.T) {
	for _, endpoint := range []string{
		"http://169.254.169.254/latest/meta-data",
		"http://[fd00:ec2::254]/",
		"http://[fe80::1]:9000/bucket",
	} {
		req := httptest.NewRequest(http.MethodGet, endpoint, nil)
		err := addressPolicy{privateAllowed: true}.checkProxiedHost(req)
		if !errors.Is(err, ErrLinkLocalEndpoint) {
			t.Errorf("%s through a proxy = %v, want ErrLinkLocalEndpoint", endpoint, err)
		}
	}
	for _, endpoint := range []string{
		"http://127.0.0.1:9000/bucket",
		"http://10.0.0.5:9000/bucket",
	} {
		req := httptest.NewRequest(http.MethodGet, endpoint, nil)
		if err := (addressPolicy{}).checkProxiedHost(req); !errors.Is(err, ErrPrivateEndpoint) {
			t.Errorf("%s through a proxy by default = %v, want ErrPrivateEndpoint", endpoint, err)
		}
		err := addressPolicy{privateAllowed: true}.checkProxiedHost(req)
		if err != nil {
			t.Errorf("%s through a proxy with private allowed = %v, want nil", endpoint, err)
		}
	}
	req := httptest.NewRequest(http.MethodGet, "http://8.8.8.8/bucket", nil)
	if err := (addressPolicy{}).checkProxiedHost(req); err != nil {
		t.Errorf("a public host through a proxy = %v, want nil", err)
	}
}

// TestRedirectsAreRefused points a store at an endpoint that answers every
// request with a 307 or 308 redirect to a second server, and checks that
// reads and writes fail and the second server never sees the signed
// request.
func TestRedirectsAreRefused(t *testing.T) {
	var redirected atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		redirected.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(target.Close)
	for _, statusCode := range []int{http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		redirecting := httptest.NewServer(http.HandlerFunc(
			func(w http.ResponseWriter, req *http.Request) {
				http.Redirect(w, req, target.URL+req.URL.Path, statusCode)
			},
		))
		t.Cleanup(redirecting.Close)
		store, err := NewStore(&NewStoreArgs{
			Provider:                ProviderS3,
			Endpoint:                redirecting.URL,
			Bucket:                  "redirecting",
			AccessKeyID:             "test",
			SecretAccessKey:         "test",
			PathStyle:               true,
			PrivateAddressesAllowed: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.Get(t.Context(), "object"); err == nil {
			t.Errorf("Get through a %d returned no error, want one", statusCode)
		}
		err = store.Put(t.Context(), "object", strings.NewReader("secret"), 6)
		if err == nil {
			t.Errorf("Put through a %d returned no error, want one", statusCode)
		}
	}
	if got := redirected.Load(); got != 0 {
		t.Fatalf("the redirect target received %d requests, want 0", got)
	}
}
