package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type config struct {
	Address           string
	DataDir           string
	ShutdownTimeout   time.Duration
	ProbeStaleAfter   time.Duration
	NotificationRetry time.Duration
	Reviewers         []reviewerCredential
}

type reviewerCredential struct {
	ID     string
	Secret string
}

func loadConfig() (config, error) {
	dataDir := os.Getenv("CRYOSAFE_DATA_DIR")
	if dataDir == "" {
		dataDir = filepath.Join(os.TempDir(), "cryosafe-data")
	}
	address := os.Getenv("CRYOSAFE_ADDR")
	if address == "" {
		address = "127.0.0.1:19698"
	}
	if address == "" || dataDir == "" {
		return config{}, errors.New("service address and data directory are required")
	}
	reviewers, err := loadReviewers()
	if err != nil {
		return config{}, err
	}
	return config{
		Address:           address,
		DataDir:           dataDir,
		ShutdownTimeout:   10 * time.Second,
		ProbeStaleAfter:   2 * time.Minute,
		NotificationRetry: 5 * time.Second,
		Reviewers:         reviewers,
	}, nil
}

// loadReviewers builds the independent reviewer credential list from
// CRYOSAFE_REVIEWERS as comma-separated id:secret pairs. It falls back to two
// local development duty accounts when unset; operators must override these in
// any real deployment.
func loadReviewers() ([]reviewerCredential, error) {
	raw := os.Getenv("CRYOSAFE_REVIEWERS")
	if strings.TrimSpace(raw) == "" {
		return []reviewerCredential{
			{ID: "duty-a", Secret: "dev-secret-a"},
			{ID: "duty-b", Secret: "dev-secret-b"},
		}, nil
	}
	var reviewers []reviewerCredential
	for _, pair := range strings.Split(raw, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		id, secret, found := strings.Cut(pair, ":")
		if !found || strings.TrimSpace(id) == "" || strings.TrimSpace(secret) == "" {
			return nil, errors.New("CRYOSAFE_REVIEWERS entries must be id:secret pairs")
		}
		reviewers = append(reviewers, reviewerCredential{ID: strings.TrimSpace(id), Secret: strings.TrimSpace(secret)})
	}
	if len(reviewers) < 2 {
		return nil, errors.New("at least two independent reviewer credentials are required")
	}
	return reviewers, nil
}
