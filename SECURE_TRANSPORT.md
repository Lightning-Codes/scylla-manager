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

All connection-affecting values are stored in one immutable, generation-keyed
`ConnectionBundle` row in `secure_connection_bundle`: host/contact points,
ports and force-TLS flags; Agent token and trust; CQL credentials, client
certificate/key and trust; and Alternator credentials and trust. This table
uses the generic Store `(cluster_id, key, value blob)` shape, but is physically
separate from the legacy `secrets` table. The active pointer and non-secret
display metadata live in `secure_cluster`. GET, list, and PUT responses never
return CA bytes,
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

Every candidate bundle is inserted immutably with `IF NOT EXISTS` before one
global-serial LWT switches `secure_cluster.connection_generation` from A to B.
A reader captures the pointer once and reads exactly that retained immutable
row; it never assembles fields from multiple keys or falls back to legacy
storage. Ambiguous writes are resolved through a fresh global-serial read; an
unavailable resolver is reported as explicitly indeterminate, never as an
uncommitted failure. Concurrent Manager replicas therefore have one LWT
winner, while a reader already holding A may finish A and new readers acquire
B.

Delete commits an empty immutable lifecycle tombstone D with the same A-to-D
LWT. The cluster UUID is then permanently retired: a later PUT for that UUID is
rejected, and a replacement cluster must be enrolled with a new UUID. Delete
does not wildcard-delete retired bundle rows. Retired bundles remain available
to readers that captured an older pointer, so deployments must encrypt the
Manager metadata store at rest and define an offline, pointer-aware retention
job before physically purging retired credential generations. `DeleteAll` is
not safe for generation rows.

This fork is greenfield-only. Before applying any ordinary Manager migration,
startup inventories the configured keyspace without mutating it. A keyspace
without the exact `scylla-manager-v3.9.1-greenfield-v1` ownership marker must
have no user tables, types, functions, aggregates, views, indexes, or triggers;
retained scheduler, migration, cluster, secret, or other Manager schema is
rejected even when its connection tables are empty.
On a truly empty keyspace Manager creates the marker in a durable `migrating`
state, applies migrations, and changes it to `ready`. Historical migrations
cannot atomically couple every DDL statement with their progress row, so an
exact owned `migrating`/`resetting` store from an interrupted first start is
never resumed in place: under the enforced single-starter deployment contract,
Manager serially marks it `resetting`, verifies it is not `ready`, drops and
recreates only that configured metadata keyspace, then replays from empty. A
crash before drop, after drop, or after recreate converges on the same clean
retry. A `ready` store is never reset. A nonempty keyspace with a missing,
incompatible, or malformed marker remains fail-closed. It has no legacy
migration, hydration, fallback, or mixed-version coexistence path. Deploy it
against a new, externally attested metadata store only.

Deleting or rotating trust/identity retires generation-keyed clients in the
local Manager process and clears its configuration cache before refresh.
Cached clients validate the active pointer at global-serial consistency, and
every health run validates the active generation before reading cached
CQL/Alternator material. Failed, partial, mixed-generation, or superseded
refreshes publish nothing. Backup, repair, restore, and health workflows pin
CQL, Alternator, and NodeConfig acquisitions to the first Agent generation.
Backup, backup-validation, vnode-repair, and tablet-repair targets record the
generation that produced their topology; backup sizing and task execution
require an exact non-zero match. A rotation between target construction and
execution aborts the operation before new credentials can reach an old
endpoint. A client that already captured A may still finish with A; killing
already-authenticated sessions on another replica requires server-side
credential revocation or an external workload drain.

## Deployment gate

The selected first deployment runs exactly one Manager replica. Before that
pod is admitted, an external rollout controller must prove that every legacy
Manager and Agent workload has terminated, the old Manager store/PVC has been
destroyed, legacy database roles and connection credentials have been revoked,
and the target store is fresh. A Kubernetes
admission policy must allow only the reviewed Manager and Agent image digests
and require the source-destruction evidence. These are deployment controls:
Manager source code cannot attest its own replica count, running image digest,
terminated workloads, destroyed storage, or revoked remote sessions.

After the greenfield gate, immutable staging plus the global-serial pointer LWT
makes bundle transitions safe with multiple Manager writers. Increasing the
replica count does not itself provide immediate cross-replica session
revocation; deployments that require that stronger cutoff must rotate or
revoke the corresponding Agent/CQL/Alternator credentials at their servers.

## Manager API and metadata store

For Operator-to-Manager mutual TLS, configure only `https`, provide
`tls_cert_file`, `tls_key_file`, and `tls_ca_file`, and issue the serving
certificate for `scylla-manager.scylla-manager.svc` (optionally also its
`.svc.cluster.local` form). `sctool` accepts `--api-ca-file`,
`--api-server-name`, `--api-cert-file`, and `--api-key-file`, with matching
`SCYLLA_MANAGER_API_*` environment variables. TLS flags with an HTTP URL are
rejected.

The complete secure server-side YAML shape is:

```yaml
http: ""
https: ":5443"
tls_cert_file: /run/secrets/manager-api/tls.crt
tls_key_file: /run/secrets/manager-api/tls.key
tls_ca_file: /run/secrets/manager-api/client-ca.crt
prometheus: ":5090"

database:
  hosts:
    - scylladb-client.scylla-manager.svc
  port: 9142
  ssl: true
  user_file: /run/secrets/manager-store/username
  password_file: /run/secrets/manager-store/password

ssl:
  cert_file: /run/secrets/manager-store/ca.crt
  validate: true
  server_name: scylladb-client.scylla-manager.svc
  user_cert_file: /run/secrets/manager-store/tls.crt
  user_key_file: /run/secrets/manager-store/tls.key
```

The API client CA is the issuer trusted for Operator/readiness client
certificates; it need not be the same CA as the API serving certificate. The
Prometheus listener remains a separate plaintext metrics endpoint and should
be reachable only from the in-cluster scraper/network policy.

The metadata Scylla credentials may be mounted through
`database.user_file` and `database.password_file`. Both files must be non-empty
regular files with no group/other permission bits. They are mutually exclusive
with inline `database.user`/`database.password`. Use `database.ssl=true`,
`ssl.cert_file=<Operator CA>`, `ssl.server_name=<stable Operator client-Service
DNS SAN>`, `ssl.validate=true`, and, when the store requires mutual TLS,
`ssl.user_cert_file` plus `ssl.user_key_file`. Authenticated metadata-store
configuration is rejected unless verified TLS has an explicit CA and server
identity; client certificate/key configuration is also atomic. When running
the supplied nonroot distroless image, projected Secret files
must be made readable by uid 65532 while retaining mode 0400/0600; use an init
or CSI copy/chown step rather than Kubernetes `fsGroup`/0440, which the strict
credential reader intentionally rejects.

The secure server image contains static `scylla-manager` and `sctool` binaries,
but intentionally contains no shell, package manager, `curl`, or CA-management
tools. Use a dedicated cert-manager-issued Manager API client certificate for
an exec readiness probe. Manager authenticates the certificate but implements
no certificate-based API authorization roles, so this identity is fully
privileged; isolate its Secret and mount the credential files read-only.
`cluster list` performs a verified mTLS API request and a metadata-store query,
so it proves more than a TCP or `/ping` check. The exact non-secret command is:

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

## Reviewed image publication

`.github/workflows/secure-manager-image.yaml` is manual-only. The dispatcher
must provide the full reviewed `expected_source_commit` and a `secure-v...`
release tag; the event SHA and every checkout must exactly equal that commit
before registry authentication or another publication side effect. Buildx,
BuildKit, QEMU, the Dockerfile frontend, actions, base images, and the Syft
version are immutable release inputs.

The workflow pushes the multi-platform result by digest only. It hashes and
verifies the OCI index, its distinct linux/amd64 and linux/arm64 child
manifests, and each child's image-config OS/architecture. A digest-pinned Syft
image generates attempt-unique per-child SPDX JSON SBOMs. The final `release`
job is protected by a pre-created `secure-image-release` GitHub environment
with a 30-minute wait timer, administrator bypass disabled, and a deployment
branch policy restricted to `sophena/secure-cluster-tls`. All manual releases
share one non-canceling concurrency group. The environment must be configured
before the first dispatch; GitHub otherwise auto-creates it without those
protections. The required authorization controls are the external
hash-specific code and workflow approvals plus the separate authorized action
that makes the newly created GHCR package Public while the environment waits;
the one-member repository cannot provide an independent in-environment
reviewer.

For the first publication, the digest-only candidate job creates the GHCR
package and then the environment gate pauses the final job. An authorized
package administrator changes that package to Public and approves the
environment. With an empty Docker credential store, the final job must then
read and hash the index and both children and pull the index digest. It attaches
per-child GitHub OIDC-signed SBOM/build-provenance attestations plus index build
provenance before creating a public tag. Only after every attestation succeeds
does it refuse any pre-existing release tag, create exactly one final tag,
rehash it, and anonymously pull it. This ordering lets a failed pre-tag
attestation run be retried without stranding an incomplete immutable release.
Subsequent releases use the same public-package and environment checks. GHCR
visibility is an external, irreversible setting; registry tag immutability and
package write permission restricted to this protected workflow are the
external hard boundary in addition to the workflow's absent-tag check. The
workflow publishes the Manager server image only, never an Agent image.

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
