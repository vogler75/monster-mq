package broker

import (
	"crypto/tls"
	"fmt"
	"os"
	"strings"
)

// loadTLS reads a PEM bundle from path. The file may contain either:
//   - one PEM block with both cert and key,
//   - or two files separated by ":" in the path (cert.pem:key.pem).
// Password parameter is reserved for future PKCS12 support.
func loadTLS(path, _ string) (*tls.Config, error) {
	if path == "" {
		return nil, fmt.Errorf("KeyStorePath is empty")
	}
	var certPath, keyPath string
	if i := strings.Index(path, ":"); i > 0 {
		certPath = path[:i]
		keyPath = path[i+1:]
	} else {
		certPath = path
		keyPath = path
	}
	if _, err := os.Stat(certPath); err != nil {
		return nil, fmt.Errorf("cert %s: %w", certPath, err)
	}
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("load keypair: %w", err)
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}, nil
}
