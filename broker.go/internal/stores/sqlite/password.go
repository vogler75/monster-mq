package sqlite

import "golang.org/x/crypto/bcrypt"

// HashPassword returns a bcrypt hash compatible with the existing Kotlin broker
// (which uses jBCrypt with the default work factor).
func HashPassword(password string) (string, error) {
	b, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
