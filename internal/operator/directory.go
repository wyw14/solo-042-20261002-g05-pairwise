package operator

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"log/slog"
	"strings"
	"sync"
)

// Directory authenticates duty operators. Credentials are stored only as
// SHA-256 digests; a reviewer therefore presents a credential the initiator's
// client never has to know.
type Directory struct {
	mu     sync.RWMutex
	byID   map[string]entry
	byHash map[[32]byte]string
}

type entry struct {
	id          string
	displayName string
	hash        [32]byte
}

// NewDirectory builds a directory from configured credentials.
func NewDirectory(credentials ...Credential) (*Directory, error) {
	directory := &Directory{
		byID:   make(map[string]entry),
		byHash: make(map[[32]byte]string),
	}
	for _, credential := range credentials {
		if err := directory.Register(credential.ID, credential.DisplayName, credential.Token); err != nil {
			return nil, err
		}
	}
	return directory, nil
}

// Register adds one operator. Tokens are never retained in clear text.
func (d *Directory) Register(id, displayName, token string) error {
	id = strings.TrimSpace(id)
	token = strings.TrimSpace(token)
	if id == "" || token == "" {
		return errors.New("operator id and credential token are required")
	}
	digest := sha256.Sum256([]byte(token))
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, exists := d.byID[id]; exists {
		return errors.New("operator already registered")
	}
	if existing, taken := d.byHash[digest]; taken {
		return errors.New("credential already assigned to operator " + existing)
	}
	d.byID[id] = entry{id: id, displayName: strings.TrimSpace(displayName), hash: digest}
	d.byHash[digest] = id
	return nil
}

// Verify checks that the token belongs to the claimed operator.
func (d *Directory) Verify(id, token string) bool {
	id = strings.TrimSpace(id)
	d.mu.RLock()
	record, ok := d.byID[id]
	d.mu.RUnlock()
	if !ok {
		// Still hash the presented token to keep timing roughly uniform.
		_ = sha256.Sum256([]byte(token))
		return false
	}
	presented := sha256.Sum256([]byte(strings.TrimSpace(token)))
	return subtle.ConstantTimeCompare(presented[:], record.hash[:]) == 1
}

// Exists reports whether an operator is registered.
func (d *Directory) Exists(id string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	_, ok := d.byID[strings.TrimSpace(id)]
	return ok
}

// Count returns the number of registered operators.
func (d *Directory) Count() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.byID)
}

// Credential is an operator id plus its clear-text token, used only while
// seeding the directory from configuration.
type Credential struct {
	ID          string
	DisplayName string
	Token       string
}

// DirectoryFromConfig parses a spec of "id:token" or "id:display-name:token"
// entries separated by commas. An empty spec seeds the two built-in duty
// positions so a development deployment still has two distinct credentials;
// callers should log a warning when falling back to them.
func DirectoryFromConfig(spec string) (*Directory, []Credential, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		fallback := []Credential{
			{ID: "duty-a", DisplayName: "Duty operator A", Token: "dev-token-a"},
			{ID: "duty-b", DisplayName: "Duty operator B", Token: "dev-token-b"},
		}
		slog.Warn("using built-in development operator credentials; set CRYOSAFE_OPERATORS for production")
		return directoryFrom(fallback)
	}
	credentials := make([]Credential, 0)
	for _, item := range strings.Split(spec, ",") {
		parts := strings.Split(strings.TrimSpace(item), ":")
		switch len(parts) {
		case 2:
			credentials = append(credentials, Credential{ID: parts[0], DisplayName: parts[0], Token: parts[1]})
		case 3:
			credentials = append(credentials, Credential{ID: parts[0], DisplayName: parts[1], Token: parts[2]})
		default:
			return nil, nil, errors.New("operator entry must be id:token or id:name:token")
		}
	}
	return directoryFrom(credentials)
}

func directoryFrom(credentials []Credential) (*Directory, []Credential, error) {
	if len(credentials) < 2 {
		return nil, nil, errors.New("at least two distinct duty operators are required")
	}
	directory := &Directory{byID: make(map[string]entry), byHash: make(map[[32]byte]string)}
	for _, credential := range credentials {
		if err := directory.Register(credential.ID, credential.DisplayName, credential.Token); err != nil {
			return nil, nil, err
		}
	}
	return directory, credentials, nil
}
