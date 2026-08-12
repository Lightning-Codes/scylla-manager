// Copyright (C) 2026 ScyllaDB

package cluster

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

const (
	cqlOpError        = byte(0x00)
	cqlOpStartup      = byte(0x01)
	cqlOpAuthenticate = byte(0x03)
	cqlOpOptions      = byte(0x05)
	cqlOpSupported    = byte(0x06)
	cqlOpQuery        = byte(0x07)
	cqlOpResult       = byte(0x08)
	cqlOpPrepare      = byte(0x09)
	cqlOpExecute      = byte(0x0a)
	cqlOpRegister     = byte(0x0b)
	cqlOpAuthResponse = byte(0x0f)
	cqlOpAuthSuccess  = byte(0x10)
)

type cqlMTLSServerState struct {
	mu             sync.Mutex
	clientVerified bool
	authVerified   bool
	queryVerified  bool
}

func (s *cqlMTLSServerState) setClientVerified() {
	s.mu.Lock()
	s.clientVerified = true
	s.mu.Unlock()
}

func (s *cqlMTLSServerState) snapshot() (bool, bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.clientVerified, s.authVerified, s.queryVerified
}

func TestValidateCQLConnectivityPerformsMutualTLSAndAuthenticatedQuery(t *testing.T) {
	caPEM, serverCert, clientCertPEM, clientKeyPEM := makeCQLMutualTLSPKI(t, "cql.internal")
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caPEM) {
		t.Fatal("append test CA")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tlsListener := tls.NewListener(listener, &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caPool,
	})
	defer tlsListener.Close()

	state := new(cqlMTLSServerState)
	serverErrors := make(chan error, 4)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := tlsListener.Accept()
			if err != nil {
				return
			}
			if err := serveAuthenticatedCQLConnection(conn, state); err != nil {
				serverErrors <- err
			}
			_ = conn.Close()
			_, _, query := state.snapshot()
			if query {
				return
			}
		}
	}()

	_, portString, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portString)
	if err != nil {
		t.Fatal(err)
	}
	c := &Cluster{
		ID:              uuid.MustRandom(),
		AuthToken:       "agent-token",
		CQLCAFile:       caPEM,
		CQLServerName:   "cql.internal",
		SSLUserCertFile: clientCertPEM,
		SSLUserKeyFile:  clientKeyPEM,
		Username:        "manager-user",
		Password:        "manager-password",
	}
	ni := &scyllaclient.NodeInfo{
		ClientEncryptionEnabled:     true,
		ClientEncryptionRequireAuth: true,
		CqlPasswordProtected:        true,
		ListenAddress:               "0.0.0.0",
		NativeTransportPortSsl:      strconv.Itoa(port),
	}
	svc := &Service{
		timeoutConfig: scyllaclient.TimeoutConfig{Timeout: 3 * time.Second},
		logger:        log.NewDevelopment(),
		// nil cqlQueryPing deliberately exercises the real gocql query path.
	}
	if err := svc.validateCQLConnectivity(context.Background(), c, "127.0.0.1", ni, true); err != nil {
		select {
		case serverErr := <-serverErrors:
			t.Fatalf("real mutual-TLS authenticated CQL query failed: %v (server: %v)", err, serverErr)
		default:
			t.Fatalf("real mutual-TLS authenticated CQL query failed: %v", err)
		}
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("CQL test server did not observe the query")
	}
	select {
	case err := <-serverErrors:
		t.Fatal(err)
	default:
	}
	clientVerified, authVerified, queryVerified := state.snapshot()
	if !clientVerified || !authVerified || !queryVerified {
		t.Fatalf("incomplete secure query proof: client_cert=%v password=%v query=%v", clientVerified, authVerified, queryVerified)
	}
}

func serveAuthenticatedCQLConnection(conn net.Conn, state *cqlMTLSServerState) error {
	for {
		var header [9]byte
		if _, err := io.ReadFull(conn, header[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		bodyLen := int(binary.BigEndian.Uint32(header[5:9]))
		body := make([]byte, bodyLen)
		if _, err := io.ReadFull(conn, body); err != nil {
			return err
		}
		if tlsConn, ok := conn.(*tls.Conn); ok {
			if err := tlsConn.Handshake(); err != nil {
				return err
			}
			if len(tlsConn.ConnectionState().PeerCertificates) == 0 {
				return fmt.Errorf("CQL client certificate was not presented")
			}
			state.setClientVerified()
		}

		var responseOpcode byte
		var responseBody []byte
		switch header[4] {
		case cqlOpOptions:
			responseOpcode = cqlOpSupported
			responseBody = []byte{0, 0} // empty string multimap
		case cqlOpStartup:
			responseOpcode = cqlOpAuthenticate
			responseBody = cqlShortString("org.apache.cassandra.auth.PasswordAuthenticator")
		case cqlOpAuthResponse:
			if len(body) < 4 {
				return fmt.Errorf("short CQL auth response")
			}
			n := int(int32(binary.BigEndian.Uint32(body[:4])))
			if n < 0 || len(body) != 4+n {
				return fmt.Errorf("invalid CQL auth response length %d", n)
			}
			if got, want := string(body[4:]), "\x00manager-user\x00manager-password"; got != want {
				return fmt.Errorf("unexpected CQL credentials")
			}
			state.mu.Lock()
			state.authVerified = true
			state.mu.Unlock()
			responseOpcode = cqlOpAuthSuccess
			responseBody = make([]byte, 4) // empty bytes value
		case cqlOpRegister:
			responseOpcode = 0x02 // READY
		case cqlOpQuery, cqlOpPrepare:
			if len(body) < 4 {
				return fmt.Errorf("short CQL query")
			}
			n := int(binary.BigEndian.Uint32(body[:4]))
			if n <= 0 || len(body) < 4+n {
				return fmt.Errorf("unexpected CQL query")
			}
			if header[4] == cqlOpPrepare {
				responseOpcode = cqlOpResult
				responseBody = cqlPreparedResult()
				break
			}
			responseOpcode = cqlOpResult
			query := string(body[4 : 4+n])
			if bytes.Contains(body[4:4+n], []byte("SELECT now() FROM system.local")) {
				state.mu.Lock()
				state.queryVerified = true
				state.mu.Unlock()
				responseBody = cqlRowsResult()
			} else if bytes.Contains(body[4:4+n], []byte("system.local")) {
				responseBody = cqlSystemLocalResult(query)
			} else {
				// gocql may issue topology discovery reads before the health query.
				// An empty, valid result makes it fall back to its explicit single
				// contact point without weakening the TLS/authentication proof.
				responseBody = cqlEmptyRowsResult()
			}
		case cqlOpExecute:
			state.mu.Lock()
			state.queryVerified = true
			state.mu.Unlock()
			responseOpcode = cqlOpResult
			responseBody = cqlRowsResult()
		default:
			responseOpcode = cqlOpError
			responseBody = cqlProtocolError(fmt.Sprintf("unsupported opcode 0x%x", header[4]))
		}
		if err := writeCQLFrame(conn, header[0]|0x80, header[2:4], responseOpcode, responseBody); err != nil {
			return err
		}
		_, _, query := state.snapshot()
		if responseOpcode == cqlOpResult && query {
			return nil
		}
	}
}

func writeCQLFrame(w io.Writer, version byte, stream []byte, opcode byte, body []byte) error {
	header := []byte{version, 0, stream[0], stream[1], opcode, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(header[5:], uint32(len(body)))
	if _, err := w.Write(header); err != nil {
		return err
	}
	_, err := w.Write(body)
	return err
}

func cqlShortString(value string) []byte {
	b := make([]byte, 2+len(value))
	binary.BigEndian.PutUint16(b, uint16(len(value)))
	copy(b[2:], value)
	return b
}

func cqlProtocolError(message string) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, 0x000a)
	return append(b, cqlShortString(message)...)
}

func cqlRowsResult() []byte {
	var b bytes.Buffer
	_ = binary.Write(&b, binary.BigEndian, int32(2)) // RESULT kind: rows
	_ = binary.Write(&b, binary.BigEndian, int32(0)) // metadata flags
	_ = binary.Write(&b, binary.BigEndian, int32(1)) // column count
	b.Write(cqlShortString("system"))
	b.Write(cqlShortString("local"))
	b.Write(cqlShortString("now"))
	_ = binary.Write(&b, binary.BigEndian, uint16(0x0003)) // blob
	_ = binary.Write(&b, binary.BigEndian, int32(1))       // row count
	_ = binary.Write(&b, binary.BigEndian, int32(8))
	b.Write(make([]byte, 8))
	return b.Bytes()
}

func cqlEmptyRowsResult() []byte {
	var b bytes.Buffer
	_ = binary.Write(&b, binary.BigEndian, int32(2)) // RESULT kind: rows
	_ = binary.Write(&b, binary.BigEndian, int32(0)) // metadata flags
	_ = binary.Write(&b, binary.BigEndian, int32(0)) // column count
	_ = binary.Write(&b, binary.BigEndian, int32(0)) // row count
	return b.Bytes()
}

func cqlPreparedResult() []byte {
	var b bytes.Buffer
	_ = binary.Write(&b, binary.BigEndian, int32(4)) // RESULT kind: prepared
	b.Write(cqlShortString("manager-health-query"))
	_ = binary.Write(&b, binary.BigEndian, int32(0)) // prepared metadata flags
	_ = binary.Write(&b, binary.BigEndian, int32(0)) // bound column count
	_ = binary.Write(&b, binary.BigEndian, int32(0)) // result metadata flags
	_ = binary.Write(&b, binary.BigEndian, int32(1)) // result column count
	b.Write(cqlShortString("system"))
	b.Write(cqlShortString("local"))
	b.Write(cqlShortString("now"))
	_ = binary.Write(&b, binary.BigEndian, uint16(0x0003))
	return b.Bytes()
}

func cqlSystemLocalResult(query string) []byte {
	type column struct {
		name   string
		typeID uint16
		value  []byte
	}
	allColumns := []column{
		{name: "key", typeID: 0x000d, value: []byte("local")},
		{name: "cluster_name", typeID: 0x000d, value: []byte("test")},
		{name: "data_center", typeID: 0x000d, value: []byte("dc1")},
		{name: "rack", typeID: 0x000d, value: []byte("rack1")},
		{name: "release_version", typeID: 0x000d, value: []byte("2026.1.0")},
		{name: "partitioner", typeID: 0x000d, value: []byte("org.apache.cassandra.dht.Murmur3Partitioner")},
		{name: "rpc_address", typeID: 0x0010, value: net.ParseIP("127.0.0.1").To4()},
		{name: "host_id", typeID: 0x000c, value: make([]byte, 16)},
		{name: "schema_version", typeID: 0x000c, value: make([]byte, 16)},
		{name: "tokens", typeID: 0x0022, value: []byte{0, 0, 0, 0}}, // empty set<text>
	}
	columns := make([]column, 0, len(allColumns))
	for _, col := range allColumns {
		if bytes.Contains([]byte(query), []byte(col.name)) {
			columns = append(columns, col)
		}
	}
	var b bytes.Buffer
	_ = binary.Write(&b, binary.BigEndian, int32(2))
	_ = binary.Write(&b, binary.BigEndian, int32(0))
	_ = binary.Write(&b, binary.BigEndian, int32(len(columns)))
	for _, col := range columns {
		b.Write(cqlShortString("system"))
		b.Write(cqlShortString("local"))
		b.Write(cqlShortString(col.name))
		if col.name == "tokens" {
			_ = binary.Write(&b, binary.BigEndian, col.typeID)
			_ = binary.Write(&b, binary.BigEndian, uint16(0x000d))
		} else {
			_ = binary.Write(&b, binary.BigEndian, col.typeID)
		}
	}
	_ = binary.Write(&b, binary.BigEndian, int32(1))
	for _, col := range columns {
		_ = binary.Write(&b, binary.BigEndian, int32(len(col.value)))
		b.Write(col.value)
	}
	return b.Bytes()
}

func makeCQLMutualTLSPKI(t *testing.T, serverName string) ([]byte, tls.Certificate, []byte, []byte) {
	t.Helper()
	now := time.Now()
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Manager CQL test CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}

	issue := func(serial int64, commonName string, dnsNames []string, usages []x509.ExtKeyUsage) ([]byte, []byte) {
		key, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatal(err)
		}
		template := &x509.Certificate{
			SerialNumber: big.NewInt(serial),
			Subject:      pkix.Name{CommonName: commonName},
			DNSNames:     dnsNames,
			NotBefore:    now.Add(-time.Hour),
			NotAfter:     now.Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
			ExtKeyUsage:  usages,
		}
		der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
		keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
		return certPEM, keyPEM
	}
	serverPEM, serverKeyPEM := issue(2, serverName, []string{serverName}, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	serverCert, err := tls.X509KeyPair(serverPEM, serverKeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	clientPEM, clientKeyPEM := issue(3, "manager-client", nil, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
	return caPEM, serverCert, clientPEM, clientKeyPEM
}
