package auth

// AccessTokenKey is the context key by which the access token of the current request should be
// stored and accessed.
var AccessTokenKey accessToken

type accessToken struct{}

// 				parser := jwt.Parser{
// 	ValidMethods: []string{"ecdsa", "rsa"},
// }
// accessToken, err := parser.Parse(rawAccessToken)
