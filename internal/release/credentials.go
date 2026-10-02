package release

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
)

// CredentialStore verifies a duty operator's independent credential.
//
// The reviewer authenticates with their own id/secret rather than the
// originator's, which is what makes the countersignature a genuinely
// independent check. Credentials are stored as salted SHA-256 hashes and
// compared in constant time.
type CredentialStore struct {
	mu      sync.RWMutex
	secrets map[string]saltedHash
}

type saltedHash struct {
	salt []byte
	hash []byte
}

// NewCredentialStore returns an empty credential store.
func NewCredentialStore() *CredentialStore {
	return &CredentialStore{secrets: make(map[string]saltedHash)}
}

// Register stores a credential for operatorID. A secret is never retained in
// plaintext.
func (c *CredentialStore) Register(operatorID, secret string) error {
	operatorID = strings.TrimSpace(operatorID)
	if operatorID == "" {
		return errors.New("operator id is required")
	}
	if strings.TrimSpace(secret) == "" {
		return errors.New("operator secret is required")
	}
	salt := deriveSalt(operatorID, secret)
	c.mu.Lock()
	c.secrets[operatorID] = saltedHash{salt: salt, hash: hashSecret(salt, secret)}
	c.mu.Unlock()
	return nil
}

// Verify reports that operatorID exists and presented the correct secret.
func (c *CredentialStore) Verify(operatorID, secret string) bool {
	operatorID = strings.TrimSpace(operatorID)
	c.mu.RLock()
	record, ok := c.secrets[operatorID]
	c.mu.RUnlock()
	if !ok {
		// Still perform a hash to reduce timing disclosure of operator
		// existence, then compare against an impossible value.
		dummy := hashSecret(deriveSalt(operatorID, secret), secret)
		_ = dummy
		return false
	}
	presented := hashSecret(record.salt, secret)
	return subtle.ConstantTimeCompare(presented, record.hash) == 1
}

// Has reports whether operatorID is registered.
func (c *CredentialStore) Has(operatorID string) bool {
	c.mu.RLock()
	_, ok := c.secrets[strings.TrimSpace(operatorID)]
	c.mu.RUnlock()
	return ok
}

func deriveSalt(operatorID, _ string) []byte {
	mac := hmac.New(sha256.New, []byte("cryosafe-release-salt"))
	mac.Write([]byte(operatorID))
	return mac.Sum(nil)
}

func hashSecret(salt []byte, secret string) []byte {
	mac := hmac.New(sha256.New, salt)
	mac.Write([]byte(secret))
	digest := mac.Sum(nil)
	encoded := make([]byte, hex.EncodedLen(len(digest)))
	hex.Encode(encoded, digest)
	return encoded
}
