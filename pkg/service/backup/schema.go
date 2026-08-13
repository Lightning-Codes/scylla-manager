// Copyright (C) 2025 ScyllaDB

package backup

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	stdErr "errors"
	"io"
	"path"
	"regexp"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/query"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// SchemaFilter allows for faster and more robust schema file lookup.
type SchemaFilter struct {
	ClusterID uuid.UUID
	TaskID    uuid.UUID
}

// GetSchema is a simple wrapper for calling GetSchema directly on Service.
func (s *Service) GetSchema(ctx context.Context, clusterID uuid.UUID, snapshotTag string, location backupspec.Location, filter SchemaFilter,
) (query.DescribedSchema, backupspec.AlternatorSchema, error) {
	client, err := s.scyllaClient(ctx, clusterID)
	if err != nil {
		return nil, backupspec.AlternatorSchema{}, errors.Wrap(err, "create client")
	}
	return GetSchema(ctx, client, snapshotTag, location, filter, s.logger)
}

// GetSchema fetches both cql and alternator backed up schema.
func GetSchema(ctx context.Context, client *scyllaclient.Client, snapshotTag string, location backupspec.Location, filter SchemaFilter, log log.Logger,
) (query.DescribedSchema, backupspec.AlternatorSchema, error) {
	host, err := getHostForLocation(ctx, client, location)
	if err != nil {
		return nil, backupspec.AlternatorSchema{}, errors.Wrap(err, "get host for location")
	}
	log.Info(ctx, "Found host for fetching schema", "host", host)

	cqlSchemaPath, alternatorSchemaPath, err := getSchemaFilePath(ctx, client, host, location, snapshotTag, filter, log)
	if err != nil {
		return nil, backupspec.AlternatorSchema{}, errors.Wrap(err, "get schema file path")
	}
	log.Info(ctx, "Found schema file path", "cql", cqlSchemaPath, "alternator", alternatorSchemaPath)

	var cqlSchema query.DescribedSchema
	if strings.HasSuffix(cqlSchemaPath, backupspec.UnsafeSchema) {
		cqlSchema, err = readLegacySchemaArchive(ctx, client, host, cqlSchemaPath)
		if err != nil {
			return nil, backupspec.AlternatorSchema{}, errors.Wrap(err, "read legacy cql schema archive")
		}
	} else {
		rawCQLSchema, err := readRawSchemaFile(ctx, client, host, cqlSchemaPath)
		if err != nil {
			return nil, backupspec.AlternatorSchema{}, errors.Wrap(err, "read cql schema file")
		}
		if err := json.Unmarshal(rawCQLSchema, &cqlSchema); err != nil {
			return nil, backupspec.AlternatorSchema{}, errors.Wrap(err, "unmarshal cql schema")
		}
	}

	var alternatorSchema backupspec.AlternatorSchema
	if alternatorSchemaPath != "" {
		rawAlternatorSchema, err := readRawSchemaFile(ctx, client, host, alternatorSchemaPath)
		if err != nil {
			return nil, backupspec.AlternatorSchema{}, errors.Wrap(err, "read alternator schema file")
		}
		if err := json.Unmarshal(rawAlternatorSchema, &alternatorSchema); err != nil {
			return nil, backupspec.AlternatorSchema{}, errors.Wrap(err, "unmarshal alternator schema")
		}
	}

	return cqlSchema, alternatorSchema, nil
}

func getHostForLocation(ctx context.Context, client *scyllaclient.Client, loc backupspec.Location) (string, error) {
	status, err := client.Status(ctx)
	if err != nil {
		return "", errors.Wrap(err, "get status")
	}
	if len(status) == 0 {
		return "", errors.New("no nodes in cluster")
	}
	if loc.DC != "" {
		status = status.Datacenter([]string{loc.DC})
	}
	if len(status) == 0 {
		return "", errors.New("no nodes in DC " + loc.DC)
	}
	return status[0].Addr, nil
}

// ErrSchemaFileNotFound is returned when no schema file is found in the backup location.
var ErrSchemaFileNotFound = errors.New("no cql schema file found in backup location")

// getSchemaFilePath looks for cql and alternator schema files in the backup location and returns their remote paths.
// If no cql schema file is found, ErrSchemaFileNotFound is returned.
// If no alternator schema file is found, alternatorSchemaPath is empty.
func getSchemaFilePath(ctx context.Context, client *scyllaclient.Client, host string, loc backupspec.Location, tag string, f SchemaFilter, log log.Logger,
) (cqlSchemaPath, alternatorSchemaPath string, err error) {
	baseDir := path.Join("backup", string(backupspec.SchemaDirKind), "cluster")
	opts := scyllaclient.RcloneListDirOpts{
		FilesOnly: true,
		Recurse:   true,
	}
	if f.ClusterID != uuid.Nil {
		baseDir = path.Join(baseDir, f.ClusterID.String())
		opts.Recurse = false
	}

	entries, err := client.RcloneListDir(ctx, host, loc.RemotePath(baseDir), &opts)
	if err != nil {
		return "", "", errors.Wrapf(err, "list directory %s on host %s", loc.RemotePath(baseDir), host)
	}

	var parseErr error
	var legacyCQLSchemaPath string
	for _, entry := range entries {
		entryTaskID, entryTag, err := ParseSchemaFileName(entry.Name)
		if err != nil {
			log.Info(ctx, "Couldn't parse schema file name", "name", entry.Name, "error", err)
			parseErr = stdErr.Join(parseErr, errors.Wrap(err, entry.Name))
			continue
		}
		if f.TaskID != uuid.Nil && f.TaskID != entryTaskID {
			continue
		}
		if tag != entryTag {
			continue
		}

		if strings.HasSuffix(entry.Name, backupspec.Schema) {
			if cqlSchemaPath != "" {
				return "", "", errors.Errorf("multiple cql schema files found (%s, %s)", cqlSchemaPath, entry.Path)
			}
			cqlSchemaPath = entry.Path
		}
		if strings.HasSuffix(entry.Name, backupspec.UnsafeSchema) {
			if legacyCQLSchemaPath != "" {
				return "", "", errors.Errorf("multiple legacy cql schema files found (%s, %s)", legacyCQLSchemaPath, entry.Path)
			}
			legacyCQLSchemaPath = entry.Path
		}
		if strings.HasSuffix(entry.Name, backupspec.AlternatorSchemaFileSuffix) {
			if alternatorSchemaPath != "" {
				return "", "", errors.Errorf("multiple alternator schema files found (%s, %s)", alternatorSchemaPath, entry.Path)
			}
			alternatorSchemaPath = entry.Path
		}
	}
	// CQL schema should always be backed up, alternator schema is optional
	if cqlSchemaPath == "" {
		cqlSchemaPath = legacyCQLSchemaPath
	}
	if cqlSchemaPath == "" {
		if parseErr != nil {
			return "", "", stdErr.Join(ErrSchemaFileNotFound, errors.Wrap(parseErr, "parse cql schema file name"))
		}
		return "", "", ErrSchemaFileNotFound
	}
	var remoteAlternatorSchemaPath string
	if alternatorSchemaPath == "" {
		log.Info(ctx, "Couldn't find alternator schema file")
	} else {
		remoteAlternatorSchemaPath = loc.RemotePath(path.Join(baseDir, alternatorSchemaPath))
	}
	return loc.RemotePath(path.Join(baseDir, cqlSchemaPath)), remoteAlternatorSchemaPath, nil
}

var schemaFileNameRegex = regexp.MustCompile(`task_(.*)_tag_(.*)_`)

const (
	taskIDSubmatchIdx      = 1
	snapshotTagSubmatchIdx = 2
	expectedSubmatchCount  = 3
)

// ParseSchemaFileName parses backed up schema file name into task ID and snapshot tag.
func ParseSchemaFileName(name string) (taskID uuid.UUID, tag string, err error) {
	name, ok := cutSchemaFileSuffix(name)
	if !ok {
		return uuid.Nil, "", errors.New("unexpected schema file suffix " + name)
	}

	m := schemaFileNameRegex.FindStringSubmatch(name)
	if len(m) != expectedSubmatchCount {
		return uuid.Nil, "", errors.New("unexpected schema file name format " + name)
	}

	taskID, err = uuid.Parse(m[taskIDSubmatchIdx])
	if err != nil {
		return uuid.Nil, "", errors.Wrapf(err, "parse task ID")
	}

	tag = m[snapshotTagSubmatchIdx]
	if !backupspec.IsSnapshotTag(tag) {
		return uuid.Nil, "", errors.New("unexpected snapshot tag format " + tag)
	}

	return taskID, tag, nil
}

func cutSchemaFileSuffix(name string) (string, bool) {
	if name, ok := strings.CutSuffix(name, backupspec.Schema); ok {
		return name, true
	}
	if name, ok := strings.CutSuffix(name, backupspec.AlternatorSchemaFileSuffix); ok {
		return name, true
	}
	if name, ok := strings.CutSuffix(name, backupspec.UnsafeSchema); ok {
		return name, true
	}
	return name, false
}

const (
	maxLegacySchemaEntries   = 1024
	maxLegacySchemaEntrySize = 16 << 20
)

var legacyKeyspaceNameRE = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// readLegacySchemaArchive converts Manager 3.4's schema.tar.gz backup format
// into schema rows understood by the current restore worker. Legacy archives
// contain one CQL file per keyspace. System keyspaces are deliberately ignored:
// a greenfield restore must retain the target cluster's auth, topology, Raft,
// and system schema rather than importing those from the retired source ring.
func readLegacySchemaArchive(ctx context.Context, client *scyllaclient.Client, host, schemaFilePath string) (schema query.DescribedSchema, err error) {
	r, err := client.RcloneOpen(ctx, host, schemaFilePath)
	if err != nil {
		return nil, errors.Wrap(err, "open legacy schema archive")
	}
	defer func() {
		err = stdErr.Join(err, errors.Wrap(r.Close(), "close legacy schema archive reader"))
	}()

	gzr, err := gzip.NewReader(r)
	if err != nil {
		return nil, errors.Wrap(err, "create legacy schema gzip reader")
	}
	defer func() {
		err = stdErr.Join(err, errors.Wrap(gzr.Close(), "close legacy schema gzip reader"))
	}()

	return parseLegacySchemaArchive(ctx, gzr)
}

func parseLegacySchemaArchive(ctx context.Context, r io.Reader) (query.DescribedSchema, error) {
	var schema query.DescribedSchema
	tr := tar.NewReader(r)
	entries := 0
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, errors.Wrap(err, "read legacy schema archive entry")
		}
		entries++
		if entries > maxLegacySchemaEntries {
			return nil, errors.Errorf("legacy schema archive exceeds %d entries", maxLegacySchemaEntries)
		}
		if h.Typeflag != tar.TypeReg || h.Name != path.Base(h.Name) || !strings.HasSuffix(h.Name, ".cql") {
			return nil, errors.Errorf("unsafe legacy schema archive entry %q", h.Name)
		}
		if h.Size < 0 || h.Size > maxLegacySchemaEntrySize {
			return nil, errors.Errorf("legacy schema archive entry %q has invalid size %d", h.Name, h.Size)
		}

		keyspace := strings.TrimSuffix(h.Name, ".cql")
		if !legacyKeyspaceNameRE.MatchString(keyspace) {
			return nil, errors.Errorf("unsafe legacy schema keyspace name %q", keyspace)
		}
		if strings.HasPrefix(strings.ToLower(keyspace), "system") {
			continue
		}
		raw, err := io.ReadAll(io.LimitReader(tr, maxLegacySchemaEntrySize+1))
		if err != nil {
			return nil, errors.Wrapf(err, "read legacy schema archive entry %q", h.Name)
		}
		if len(raw) == 0 || len(raw) > maxLegacySchemaEntrySize {
			return nil, errors.Errorf("legacy schema archive entry %q has invalid content size %d", h.Name, len(raw))
		}
		schema = append(schema, query.DescribedSchemaRow{
			Keyspace: keyspace,
			Type:     "legacy_keyspace",
			Name:     keyspace,
			CQLStmt:  string(raw),
		})
	}
	if len(schema) == 0 {
		return nil, errors.New("legacy schema archive contains no user keyspace")
	}
	return schema, nil
}

// readRawSchemaFile fetches and decompresses schema file from the backup location.
// It works for both cql and alternator schema files.
func readRawSchemaFile(ctx context.Context, client *scyllaclient.Client, host, schemaFilePath string) (rawSchema []byte, err error) {
	r, err := client.RcloneOpen(ctx, host, schemaFilePath)
	if err != nil {
		return nil, errors.Wrap(err, "open schema file")
	}
	defer func() {
		err = stdErr.Join(err, errors.Wrap(r.Close(), "close schema file reader"))
	}()

	gzr, err := gzip.NewReader(r)
	if err != nil {
		return nil, errors.Wrap(err, "create gzip reader")
	}
	defer func() {
		err = stdErr.Join(err, errors.Wrap(gzr.Close(), "close gzip reader"))
	}()

	rawSchema, err = io.ReadAll(gzr)
	if err != nil {
		return nil, errors.Wrap(err, "decompress schema")
	}
	return rawSchema, nil
}
