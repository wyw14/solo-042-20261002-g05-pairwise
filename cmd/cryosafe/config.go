package main

import (
	"errors"
	"os"
	"path/filepath"
	"time"
)

type config struct {
	Address           string
	DataDir           string
	ShutdownTimeout   time.Duration
	ProbeStaleAfter   time.Duration
	NotificationRetry time.Duration
	Operators         string
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
	return config{
		Address:           address,
		DataDir:           dataDir,
		ShutdownTimeout:   10 * time.Second,
		ProbeStaleAfter:   2 * time.Minute,
		NotificationRetry: 5 * time.Second,
		Operators:         os.Getenv("CRYOSAFE_OPERATORS"),
	}, nil
}
