package auth

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"gopkg.in/square/go-jose.v2/jwt"
)

// Validator unmarshals and validates tokens. It uses a key from a provided key source to verify
// the token's signature, and verifies the token's intended source and recipient against a static
// issuer and audience respectively.
type Validator struct {
	source   KeySource
	issuer   string
	realm    string
	audience jwt.Audience
}

// NewValidator returns a new validator.
func NewValidator(source KeySource, issuer, realm string, audience []string) *Validator {
	if source == nil {
		panic("source is required")
	}
	if issuer == "" {
		panic("issuer is required")
	}
	if realm == "" {
		panic("realm is required")
	}
	if len(audience) == 0 {
		panic("audience is required")
	}
	return &Validator{
		source:   source,
		issuer:   issuer,
		realm:    realm,
		audience: audience,
	}
}

// Token is a validated access token. To create a token, either call UnmarshalAndValidate on a
// Validator with an existing serialized token, or create a new one by simply declaring a Token
// variable.
type Token struct {
	User   string       `json:"aud"`
	Topics []TopicClaim `json:"deq.katchecode.com/topics"`
}

// TopicClaim defines permissions for a range of topics and values in those topics.
type TopicClaim struct {
	// Names defines the topics the claim applies to. Required.
	Names Range `json:"names"`
	// Keys defines the claim applies to for the range of topics. Required.
	Keys Range `json:"keys"`
	// Read declares that the user may read in this key and value range. At least one of Read or Write
	// is required.
	Read bool `json:"read"`
	// Write declares that the user may write in this key and value range. At least one of Read or
	// Write is  required.
	Write bool `json:"write"`
}

// Range defines a range of values that a claim applies to.
type Range struct {
	// Min defines the minimum value of the range, inclusive. It may be used with Max, but not All or
	// Only
	Min string
	// Max defines the maximum value of the range, exclusive. It may be used with Min, but not All or
	// Only.
	Max string
	// All declares that all values are considered in the range. It may not be used with Min, Max, or
	// Only.
	All bool
	// Only defines a single value that is in the range. It may not be used with Min, Max, or All.
	Only string
}

// UnmarshalAndValidate validates a token which has been signed and marshalled.
// If valid, the unmarshalled token is returned. Otherwise, an error is returned.
func (v *Validator) UnmarshalAndValidate(ctx context.Context, raw string) (*Token, error) {
	tok, err := jwt.ParseSigned(raw)
	if err != nil {
		return nil, fmt.Errorf("parse token: %w", err)
	}
	if len(tok.Headers) == 0 {
		return nil, errors.New("missing token header")
	}
	header := tok.Headers[0]

	jwks, err := v.source.KeySet(ctx)
	if err != nil {
		return nil, fmt.Errorf("get key set: %w", err)
	}

	var publicKey interface{}
	for _, key := range jwks.Key(header.KeyID) {
		if !key.IsPublic() {
			continue
		}
		if key.Use != "" && key.Use != "sig" {
			continue
		}
		if key.Algorithm != header.Algorithm {
			continue
		}
		publicKey = key.Key
		break
	}
	if publicKey == nil {
		return nil, fmt.Errorf("get key: no suitable key for kid %q", header.KeyID)
	}

	var stdClaims jwt.Claims
	var appClaims Token
	if err := tok.Claims(publicKey, &stdClaims, &appClaims); err != nil {
		return nil, fmt.Errorf("validate signature: %w", err)
	}

	err = stdClaims.Validate(jwt.Expected{
		Issuer:   v.issuer,
		Audience: v.audience,
		ID:       "",
		Time:     time.Now(),
	})
	if err != nil {
		return nil, fmt.Errorf("validate claims: %w", err)
	}

	return &appClaims, nil
}

// UnmarshalAndValidateHeader validates a header in the bearer token format. If
// the header is validated, the unmarshalled token is returned, otherwise an
// error is returned.
func (v *Validator) UnmarshalAndValidateHeader(ctx context.Context, authHeader []string) (*Token, error) {
	// Verify Authorization header exists.
	if len(authHeader) == 0 {
		return nil, ErrMissingToken
	}

	// Reject if multiple authorization headers are set
	if len(authHeader) > 1 {
		return nil, NewChallengeError("invalid_token", "multiple authorization headers set")
	}

	// Authorization header exists, verify it has a "bearer" token in a valid format.
	authorization := strings.SplitN(authHeader[0], " ", 2)
	if len(authorization) < 2 || strings.ToLower(authorization[0]) != "bearer" {
		return nil, NewChallengeError("invalid_token", "authorization header is not a valid bearer token")
	}

	// Unmarshal and verify bearer token
	rawAccessToken := authorization[1]
	accessToken, err := v.UnmarshalAndValidate(ctx, rawAccessToken)
	if err != nil {
		return nil, NewChallengeError("invalid_token", "bearer token is invalid")
	}

	return accessToken, nil
}

// ChallengeString returns a challenge string for v, in. If err does not equal
// nil or ErrMissingToken, it is used as a reason for why the current challenge
// attempt failed.
func (v *Validator) ChallengeString(err error, scopes []string) string {
	scopesStr := strings.Join(scopes, ",")

	if err == nil || err == ErrMissingToken {
		return fmt.Sprintf("realm=%q,service=%q,scopes=%q", v.realm, v.audience[0], scopesStr)
	}

	errtype, errmsg := "unknown", err.Error()
	if cerr, ok := err.(challengeError); ok {
		errtype = cerr.Type
		errmsg = cerr.Msg
	}

	return fmt.Sprintf("realm=%q,service=%q,scopes=%q,error=%q,error_description=%q", v.realm, v.audience[0], scopesStr, errtype, errmsg)
}

type challengeError struct {
	Type string
	Msg  string
}

func (err challengeError) Error() string {
	return fmt.Sprintf("error=%q,error_description=%q", err.Type, err.Msg)
}

func NewChallengeError(errtype, msg string) error {
	return challengeError{
		Type: errtype,
		Msg:  msg,
	}
}

var (
	ErrMissingToken = NewChallengeError("missing_token", "")
)
