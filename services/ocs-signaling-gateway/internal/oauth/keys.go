package oauth

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"time"
)

// KeySet is the set of public keys the NRF signs access tokens with. It is
// immutable; a rotation builds a new one (see Verifier.SetKeys).
type KeySet struct {
	keys []crypto.PublicKey
}

// Len is the number of usable keys.
func (k *KeySet) Len() int { return len(k.keys) }

const minRSABits = 2048

// ParseKeySet reads PEM blocks of type CERTIFICATE, PUBLIC KEY or RSA PUBLIC
// KEY. Accepted keys are ECDSA P-256 / P-384 and RSA >= 2048 bits. Expired or
// not-yet-valid certificates are skipped (and reported in skipped), because a
// rotation file normally still contains the retiring certificate; it is an
// error only if no usable key remains. Trust comes from the operator placing
// the NRF's certificate or key here, not from a certificate chain.
func ParseKeySet(pemData []byte, now time.Time) (ks *KeySet, skipped []string, err error) {
	ks = &KeySet{}
	rest := pemData
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		var pub any
		switch block.Type {
		case "CERTIFICATE":
			cert, perr := x509.ParseCertificate(block.Bytes)
			if perr != nil {
				return nil, nil, fmt.Errorf("oauth: bad certificate: %w", perr)
			}
			if now.Before(cert.NotBefore) || now.After(cert.NotAfter) {
				skipped = append(skipped, fmt.Sprintf("certificate %q outside its validity period", cert.Subject.String()))
				continue
			}
			pub = cert.PublicKey
		case "PUBLIC KEY":
			pub, err = x509.ParsePKIXPublicKey(block.Bytes)
		case "RSA PUBLIC KEY":
			pub, err = x509.ParsePKCS1PublicKey(block.Bytes)
		default:
			continue // private keys and anything else in a bundle are ignored
		}
		if err != nil {
			return nil, nil, fmt.Errorf("oauth: bad public key: %w", err)
		}
		switch k := pub.(type) {
		case *ecdsa.PublicKey:
			if k.Curve != elliptic.P256() && k.Curve != elliptic.P384() {
				return nil, nil, fmt.Errorf("oauth: unsupported curve %s", k.Curve.Params().Name)
			}
		case *rsa.PublicKey:
			if k.N.BitLen() < minRSABits {
				return nil, nil, fmt.Errorf("oauth: RSA key of %d bits is below the %d-bit minimum", k.N.BitLen(), minRSABits)
			}
		default:
			return nil, nil, fmt.Errorf("oauth: unsupported key type %T", pub)
		}
		ks.keys = append(ks.keys, pub.(crypto.PublicKey))
	}
	if len(ks.keys) == 0 {
		return nil, skipped, errors.New("oauth: no usable public key found")
	}
	return ks, skipped, nil
}

// LoadKeySet reads a PEM file (see ParseKeySet).
func LoadKeySet(path string, now time.Time) (*KeySet, []string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	return ParseKeySet(b, now)
}
