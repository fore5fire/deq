package main

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"net/url"
)

// Algorithm is a JWK algorithm, used in the 'alg' field of a JWK.
type Algorithm string

// Supported algorithms
const (
	RS256 Algorithm = "RS256"
	RS384 Algorithm = "RS384"
	RS512 Algorithm = "RS512"

	ES256 Algorithm = "ES256"
	ES384 Algorithm = "ES384"
	ES512 Algorithm = "ES512"
)

var (
	// ErrUnsupportedKey is returned when using a key not used in a supported algorithm.
	ErrUnsupportedKey = errors.New("unsupported key")
	// ErrKeyRequired is returned when using a JSONWebKey whose Key is nil.
	ErrKeyRequired = errors.New("key required")
	// ErrUnsupportedUse is returned when using a JSONWebKey whose Use is set but not "sig"
	ErrUnsupportedUse = errors.New("unsupported use")
)

// JSONWebKey is a parsed JSON Web Key as defined by RFC7517 (https://tools.ietf.org/html/rfc7517).
type JSONWebKey struct {
	Key           interface{}
	KeyID         string
	Algorithm     string
	Use           string
	KeyOperations []string

	// CertChain is the certificates needed to validate that this JWK's certificate, if provided.
	CertChain []*x509.Certificate
	// CertURL is a URL to an X.509 certificate or certificate chain, if provided.
	CertURL *url.URL
	// CertThumbprintSHA1 is the raw base64url encoded SHA1 hash of the PEM encoded certificate, if
	// provided.
	CertThumbprintSHA1 string
	// CertThumbprintSHA256 is the raw base64url encoded SHA256 hash of the PEM encoded certificate,
	// if provided.
	CertThumbprintSHA256 string
}

// MarshalJWK marshals jwk into a byte slice as a JSON Web Key and returns it.
func MarshalJWK(jwk JSONWebKey) ([]byte, error) {

	if jwk.Key == nil {
		return nil, ErrKeyRequired
	}
	if jwk.Use != "" && jwk.Use != "sig" {
		return nil, ErrUnsupportedUse
	}
	// TODO: check jwk.KeyOperations for valid values

	raw := rawJSONWebKey{
		KeyID:                jwk.KeyID,
		Use:                  jwk.Use,
		KeyOperations:        jwk.KeyOperations,
		X509CertChain:        make([][]byte, len(jwk.CertChain)),
		X509SHA1Thumbprint:   jwk.CertThumbprintSHA1,
		X509SHA256Thumbprint: jwk.CertThumbprintSHA256,
	}

	if jwk.CertURL != nil {
		raw.X509URL = jwk.CertURL.String()
	}

	for i, cert := range jwk.CertChain {
		var buf bytes.Buffer
		pem.Encode(&buf, &pem.Block{
			Type:  "CERTIFICATE",
			Bytes: cert.Raw,
		})
		raw.X509CertChain[i] = buf.Bytes()
	}

	switch key := jwk.Key.(type) {
	case *rsa.PublicKey:
		raw.KeyType = "RSA"
		switch key.Size() {
		case 256:
			raw.Algorithm = RS256
		case 384:
			raw.Algorithm = RS384
		case 521:
			raw.Algorithm = RS512
		default:
			return nil, ErrUnsupportedKey
		}
	case *ecdsa.PublicKey:
		raw.KeyType = "EC"
		// Pad x to the full bit size
		x := padFront0(key.X.Bytes(), key.Params().BitSize)

		raw.ECx = base64.URLEncoding.EncodeToString(x)
		switch key.Params().BitSize {
		case 256:
			raw.Algorithm = ES256
			raw.ECcrv = curveP256
		case 384:
			raw.Algorithm = ES384
			raw.ECcrv = curveP384
		case 512:
			raw.Algorithm = ES512
			raw.ECcrv = curveP521
		default:
			return nil, ErrUnsupportedKey
		}
	default:
		return nil, fmt.Errorf("unsupported key type %T", jwk.Key)
	}

}

// UnmarshalJWK unmarshals a JSON Web Key into jwk.
func UnmarshalJWK(src []byte, dst *JSONWebKey) error {

}

type JSONWebKeySet []*JSONWebKey

// MarshalJWKSet marshals JSON Web Key Set into a byte slice as JSON data and returns it.
func MarshalJWKSet(jwks JSONWebKeySet) ([]byte, error) {

}

// UnmarshalJWKSet unmarshals a JSON Web Key Set in src into jwk.
func UnmarshalJWKSet(src []byte, dst *JSONWebKeySet) {

}

type keyType string

// Supported key types
const (
	keyTypeECDSA keyType = "EC"
	keyTypeRSA   keyType = "RSA"
)

type curve string

// Supported elliptic curves
const (
	curveP256 curve = "P-256"
	curveP384 curve = "P-384"
	curveP521 curve = "P-521"
)

type rawJSONWebKey struct {
	KeyType keyType `json:"kty"`
	Use     string  `json:"use"`
	// Required with KeyType == "EC"
	ECx string `json:"x"`
	// Required with KeyType == "EC"
	ECy string `json:"y"`
	// Required with KeyType == "EC"
	ECcrv curve `json:"crv"`
	// Must not be set with KeyType == "EC" - would indicate a private key
	ECd                  string    `json:"d"`
	KeyOperations        []string  `json:"key_ops"`
	Algorithm            Algorithm `json:"alg"`
	KeyID                string    `json:"kid"`
	X509URL              string    `json:"x5u"`
	X509CertChain        [][]byte  `json:"x5c"`
	X509SHA1Thumbprint   string    `json:"x5t"`
	X509SHA256Thumbprint string    `json:"x5t#S256"`
}

// padFront0 pads the leading bits of a byte slice with zeros so that it has a size of totalBits. If
// totalBits is not exactly divisible by 8, the number of bits to pad is rounded up to the nearest
// multiple of 8.
func padFront0(buf []byte, totalBits int) []byte {
	// Calculate total length in bytes, rounding up.
	byteLen := totalBits / 8
	if totalBits%8 > 0 {
		byteLen++
	}

	// If buf is at least byteLen then no change is needed.
	if len(buf) >= byteLen {
		return buf
	}

	// Setup our final slice with the correct size.
	// If the underlying array already has sufficient capacity, just reuse it.
	var padded []byte
	if cap(buf) >= byteLen {
		padded = buf[:byteLen]
	} else {
		padded = make([]byte, byteLen)
	}

	// Copy the data to the new array, skipping space for the padding.
	copy(padded[byteLen-len(buf):], buf)
	return padded
}
