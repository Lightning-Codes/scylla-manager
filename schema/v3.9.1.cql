-- Greenfield secure cluster metadata. Startup rejects any unowned/preexisting
-- Manager schema before ordinary migrations reach this idempotent DDL.
CREATE TABLE IF NOT EXISTS secure_cluster (
    id uuid PRIMARY KEY,
    name text,
    labels map<text, text>,
    connection_generation uuid,
    connection_deleted boolean,
    lifecycle_epoch bigint
);

-- Generic Store row shape for immutable generation-qualified connection
-- bundles, physically isolated from all other Manager state.
CREATE TABLE IF NOT EXISTS secure_connection_bundle (
    cluster_id uuid,
    key text,
    value blob,
    PRIMARY KEY (cluster_id, key)
);
