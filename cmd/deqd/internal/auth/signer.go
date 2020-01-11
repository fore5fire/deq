package auth

type Signer struct {
	source KeySource
}

func NewSigner(source KeySource) *Signer {
	return &Signer{
		source: source,
	}
}

func (s *Signer) MarshalAndSign(tkn *Token) (string, error) {

}
