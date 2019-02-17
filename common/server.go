package common

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
)

// RunServer runs an http.Server with the specified http.Handler in a
// goroutine. It will optionally enable TLS.
func RunServer(
	tlsConfig *TLSConfig,
	handler http.Handler,
	wg *sync.WaitGroup,
	addr string,
	insecure bool,
) *http.Server {
	server := &http.Server{
		Addr:    addr,
		Handler: handler,
	}

	if insecure {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				panic(err)
			}
		}()
	} else {
		cert, err := ioutil.ReadFile(tlsConfig.CertFile)
		if err != nil {
			panic(err)
		}
		certPool := x509.NewCertPool()
		certPool.AppendCertsFromPEM(cert)

		config := &tls.Config{
			ClientCAs:  certPool,
			ClientAuth: tls.RequireAndVerifyClientCert,
		}
		config.BuildNameToCertificate()
		server.TLSConfig = config

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := server.ListenAndServeTLS(
				tlsConfig.CertFile,
				tlsConfig.KeyFile,
			); err != http.ErrServerClosed {
				panic(err)
			}
		}()
	}
	return server
}

// AcceptsMimeType returns whether the provided MIME type was mentioned in the
// Accept HTTP header in the http.Request.
func AcceptsMimeType(r *http.Request, mimeType string) bool {
	for _, accepts := range r.Header["Accept"] {
		for _, mime := range strings.Split(accepts, ",") {
			if strings.TrimSpace(mime) == mimeType {
				return true
			}
		}
	}
	return false
}
