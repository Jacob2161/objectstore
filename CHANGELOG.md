# Changelog

## v0.3.0 (2026-11-28)

- Requires Go 1.27, the newest Go release; Go 1.26 is no longer
  supported.
- CI runs staticcheck and govulncheck at the versions pinned in
  `tools/go.mod`, a module of its own that importers never see.

## v0.2.0 (2026-11-28)

Breaking for callers that use private or loopback bucket endpoints:

- s3store denies endpoints that resolve to private, loopback, or
  unspecified addresses unless `NewStoreArgs.PrivateAddressesAllowed` is
  set, and link-local and cloud metadata addresses always. Set the field
  for a bucket on a LAN or on this host.
- s3store follows no redirect: every request carries a signature, which a
  redirect would resend wherever it points. Configure the final endpoint.
- A proxy from the environment may be on a private address; the policy
  applies to the bucket it carries a request to, and a bucket host this
  machine cannot resolve is denied before the proxy is asked.

## v0.1.1

- Tests use neutral object keys.

## v0.1.0

- First release: the `Store` interface, `TestBucket`, `filestore`, and
  `s3store`.
