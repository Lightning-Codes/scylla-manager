# Secure transport contract

This branch is based on Scylla Manager v3.9.1 and keeps the upstream
`github.com/scylladb/scylla-manager/v3` import paths.

## Cluster registration

Secure cluster registration requires a non-empty Agent bearer token and an
Agent CA/server-name pair. CQL and Alternator CA/server-name pairs are required
when those endpoints advertise TLS. The REST fields are:

- `cql_ca_file` and `cql_server_name`
- `alternator_ca_file` and `alternator_server_name`
- `agent_ca_file` and `agent_server_name`

Each pair is atomic and stored under a protocol-specific key in Manager's
generic secrets table. GET, list, and PUT responses never return CA bytes,
server names, credentials, tokens, or client keys. They return only
`*_ca_set`, `*_credentials_set`, `auth_token_set`, and
`ssl_user_cert_set` presence flags.

When CQL password authentication or Alternator authorization is advertised,
the endpoint must also advertise TLS. Manager refuses to send credentials over
plaintext. Unauthenticated plaintext endpoints remain a legacy-compatible
configuration, but their TLS/auth verified status is false.

The Sophena Operator contract uses one CA selector and one explicit Agent
server name. Operator v1.21.1 must issue one common multi-SAN serving
certificate to every Agent. It must include every member Service ClusterIP and
member DNS name plus both the stable shared identity DNS SAN
`scylladb-client.sophena.svc` and its configured-cluster-domain FQDN. Manager
may dial discovered member Service IPs
while verifying that explicit shared DNS identity; it never disables hostname
or chain verification. If the Operator stops including the shared SAN or stops
mounting the common identity, registration must fail until this API is extended
with an endpoint-to-identity mapping.

The Agent itself requires explicit `tls_cert_file` and `tls_key_file`
configuration. It no longer generates an ephemeral self-signed serving
certificate, because such a certificate cannot participate in stable CA trust
or safe rotation.

The repository's Docker integration fixture uses the same generated CA-backed
serving certificate on every Agent and verifies the shared test identity
`scylla-manager-agent.test`. Its existing CA generation step must run before
deploying the Agent fixture.

Successful per-node status is explicit:

- `agent_tls_verified` is true only after a protected Agent REST request
  succeeds with the stored bearer token over verified TLS.
- `cql_tls_verified` and `cql_auth_verified` reflect the successful CQL probe;
  required authentication never falls back to OPTIONS.
- `alternator_tls_verified` and `alternator_auth_verified` reflect a successful
  real DynamoDB query, SigV4-signed with stored credentials when authorization
  is enabled.

Deleting or rotating trust/identity invalidates clients and clears the config
cache before refresh. Failed, partial, or superseded refreshes publish nothing.

Cluster row and secret-bundle mutations are serialized inside the Manager
process so concurrent rotations cannot publish a mixed bundle. The metadata
schema has no cross-table transaction joining the cluster row to generic
secrets, so this fork must run as a single Manager writer replica. Multi-writer
deployment remains unsupported until the schema gains a versioned/LWT bundle
transaction.

The existing generic-secrets schema also has no atomic multi-key bundle read or
write. Writer serialization prevents mutation/rollback interleaving, and
post-commit caches are revoked before network work, but an already-running
reader can overlap the short sequence of secret writes. A future schema must
stage a complete versioned secret bundle and atomically switch one version
pointer before this can provide transaction-level rotation across concurrent
probes. Operators should quiesce tasks/probes during trust rotation.

## Manager API and metadata store

For Operator-to-Manager mutual TLS, configure only `https`, provide
`tls_cert_file`, `tls_key_file`, and `tls_ca_file`, and issue the serving
certificate for `scylla-manager.scylla-manager.svc` (optionally also its
`.svc.cluster.local` form). `sctool` accepts `--api-ca-file`,
`--api-server-name`, `--api-cert-file`, and `--api-key-file`, with matching
`SCYLLA_MANAGER_API_*` environment variables. TLS flags with an HTTP URL are
rejected.

The metadata Scylla credentials may be mounted through
`database.user_file` and `database.password_file`. Both files must be non-empty
regular files with no group/other permission bits. They are mutually exclusive
with inline `database.user`/`database.password`. Use `database.ssl=true`,
`ssl.cert_file=<Operator CA>`, and `ssl.validate=true` for the store transport.
Authenticated metadata-store configuration is rejected unless all three are
set. When running the supplied nonroot distroless image, projected Secret files
must be made readable by uid 65532 while retaining mode 0400/0600; use an init
or CSI copy/chown step rather than Kubernetes `fsGroup`/0440, which the strict
credential reader intentionally rejects.

The secure server image contains static `scylla-manager` and `sctool` binaries,
but intentionally contains no shell, package manager, `curl`, or CA-management
tools. Use a dedicated cert-manager-issued, read-only Manager API client
certificate for an exec readiness probe. `cluster list` performs a verified
mTLS API request and a metadata-store query, so it proves more than a TCP or
`/ping` check. With the readiness client identity mounted read-only, the exact
non-secret command is:

```yaml
readinessProbe:
  exec:
    command:
      - /usr/bin/sctool
      - --api-url=https://127.0.0.1:5443/api/v1
      - --api-ca-file=/run/secrets/manager-readiness/ca.crt
      - --api-server-name=scylla-manager.scylla-manager.svc
      - --api-cert-file=/run/secrets/manager-readiness/tls.crt
      - --api-key-file=/run/secrets/manager-readiness/tls.key
      - cluster
      - list
  periodSeconds: 10
  timeoutSeconds: 5
```

The serving certificate must contain the configured DNS identity even though
the probe dials loopback. The readiness client certificate must chain to
`tls_ca_file`; no credential value appears in the process arguments.

## Multi-module generation constraint

This repository contains separate root, Swagger, and managerclient Go modules.
The branch uses local `replace` directives and a regenerated vendor tree so one
source branch is buildable before module publication. An upstream release must
instead publish in order:

1. regenerate and publish `v3/swagger` (generator: go-swagger v0.25.0);
2. update/publish `v3/pkg/managerclient` against that Swagger version;
3. update the root module versions and run `go mod vendor`;
4. remove the branch-local `replace` directives.

Do not publish the nested modules with their local replacement in place.
