package replication_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/txix-open/isp-kit/log"
	"github.com/txix-open/walx/replication"
	"os"
	"path"
	"sync/atomic"
	"testing"
	"time"
)

func TestClientTls(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	logger, err := log.New()
	require.NoError(err)

	serverCert, ca, err := tlsConfig("testdata/certificates/server")
	require.NoError(err)
	masterWal := createWal(t, require)
	srv := replication.NewServer(masterWal, logger, replication.ServerTls(serverConfig(serverCert, ca)))
	lis, addr := listener(require)
	go func() {
		err := srv.Serve(lis)
		require.NoError(err)
	}()

	time.Sleep(100 * time.Millisecond)

	clientCert, ca, err := tlsConfig("testdata/certificates/client")
	require.NoError(err)
	clientWal := createWal(t, require)
	cli := replication.NewClient(clientWal, "test", addr, []string{"test"}, logger, replication.ClientTls(clientConfig(clientCert, ca)))
	go func() {
		err := cli.Run(context.Background())
		require.NoError(err)
	}()

	clientReader := clientWal.OpenReader(0)
	counter := &atomic.Int64{}
	go func() {
		for {
			_, err := clientReader.Read()
			require.NoError(err)
			counter.Add(1)
		}
	}()

	for i := 0; i < 100; i++ {
		_, err := masterWal.Write(append([]byte{4}, []byte("testhello")...), func(index uint64) {

		})
		require.NoError(err)
	}

	time.Sleep(1 * time.Second)
	require.NoError(masterWal.Close())

	require.EqualValues(100, counter.Load())
}

func tlsConfig(dir string) (tls.Certificate, *x509.CertPool, error) {
	cert, err := tls.LoadX509KeyPair(path.Join(dir, "cert.pem"), path.Join(dir, "key.pem"))
	if err != nil {
		return tls.Certificate{}, nil, err
	}

	pool := x509.NewCertPool()
	ca, err := os.ReadFile("testdata/certificates/ca/ca.pem")
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	if !pool.AppendCertsFromPEM(ca) {
		return tls.Certificate{}, nil, errors.New("failed to append ca cert")
	}

	return cert, pool, nil
}

func serverConfig(cert tls.Certificate, ca *x509.CertPool) *tls.Config {
	return &tls.Config{
		ClientAuth:   tls.RequireAndVerifyClientCert,
		Certificates: []tls.Certificate{cert},
		ClientCAs:    ca,
	}
}

func clientConfig(cert tls.Certificate, ca *x509.CertPool) *tls.Config {
	return &tls.Config{
		ServerName:   "server.txix.ru",
		Certificates: []tls.Certificate{cert},
		RootCAs:      ca,
	}
}
