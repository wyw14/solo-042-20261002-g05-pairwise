package fill

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/valve"
)

type RetryPolicy struct {
	Attempts int
	Delay    time.Duration
}

type RetryService struct {
	commands *valve.CommandService
	policy   RetryPolicy
}

func NewRetryService(commands *valve.CommandService, policy RetryPolicy) (*RetryService, error) {
	if commands == nil || policy.Attempts < 1 || policy.Delay < 0 {
		return nil, errors.New("invalid retry service configuration")
	}
	return &RetryService{commands: commands, policy: policy}, nil
}

func (s *RetryService) Open(ctx context.Context, sessionID uuid.UUID, generation uint64, valveID string) (model.ValveReceipt, error) {
	return s.OpenCommand(ctx, uuid.New(), sessionID, generation, valveID)
}

// OpenCommand retries opening a valve with a caller-assigned command id. Used
// by pre-authorised releases, which journal the command identity before the
// command is issued.
func (s *RetryService) OpenCommand(ctx context.Context, commandID, sessionID uuid.UUID, generation uint64, valveID string) (model.ValveReceipt, error) {
	var lastErr error
	for attempt := 0; attempt < s.policy.Attempts; attempt++ {
		receipt, err := s.commands.Open(ctx, commandID, sessionID, generation, valveID)
		if err == nil {
			return receipt, nil
		}
		lastErr = err
		if attempt+1 < s.policy.Attempts {
			timer := time.NewTimer(s.policy.Delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return model.ValveReceipt{}, ctx.Err()
			case <-timer.C:
			}
		}
	}
	return model.ValveReceipt{}, lastErr
}
