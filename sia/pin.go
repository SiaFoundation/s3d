package sia

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/SiaFoundation/s3d/sia/objects"
	"go.sia.tech/core/types"
	"go.uber.org/zap"
)

const (
	// pinRetryBackoff is the delay between pin attempts for a single
	// object after a transient failure.
	pinRetryBackoff = 5 * time.Minute

	// pinBatchSize is the maximum number of unpinned objects processed
	// per pin loop iteration.
	pinBatchSize = 100
)

// wakePinLoop signals the pin loop to run an iteration immediately rather than
// continue sleeping. Safe to call concurrently; if a wake is already pending
// the call is a no-op.
func (s *Sia) wakePinLoop() {
	select {
	case s.pinWake <- struct{}{}:
	default:
	}
}

// pinLoop processes pending pin requests, sleeping until the next due row's
// next_attempt_at or until woken by wakePinLoop.
func (s *Sia) pinLoop(ctx context.Context) {
	if s.uploadDisabled {
		return
	}

	for {
		err := s.performObjectPinning(ctx)
		if ctx.Err() != nil {
			return
		} else if err != nil {
			s.logger.Error("failed to pin objects", zap.Error(err))
			select {
			case <-ctx.Done():
				return
			case <-time.After(pinRetryBackoff):
			case <-s.pinWake:
			}
			continue
		}

		next, ok, err := s.store.NextPinningAttempt()

		// upon error, retry again after a backoff delay
		if err != nil {
			s.logger.Error("failed to query next pinning attempt", zap.Error(err))
			select {
			case <-ctx.Done():
				return
			case <-time.After(pinRetryBackoff):
			case <-s.pinWake:
			}
			continue
		}

		// if we ran out of pending pins, wait for a wake signal
		if !ok {
			select {
			case <-ctx.Done():
				return
			case <-s.pinWake:
			}
			continue
		}

		// otherwise, sleep until the next attempt is due
		delay := time.Until(next)
		if delay <= 0 {
			continue
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		case <-s.pinWake:
			timer.Stop()
		}
	}
}

// performObjectPinning fetches due unpinned objects in batches and pins them.
// Rows whose pin_before deadline has passed are scheduled for re-upload
// instead. Unexpected database errors are returned so the pin loop can apply
// a backoff before retrying; transient pin failures bump the row's
// next_attempt_at so the rest of the queue keeps moving.
func (s *Sia) performObjectPinning(ctx context.Context) error {
	s.pinMu.Lock()
	defer s.pinMu.Unlock()

	for ctx.Err() == nil {
		now := time.Now()
		toPin, err := s.store.ObjectsForPinning(now, pinBatchSize)
		if err != nil {
			return fmt.Errorf("failed to fetch unpinned objects: %w", err)
		} else if len(toPin) == 0 {
			return nil
		}

		for _, uo := range toPin {
			if ctx.Err() != nil {
				return nil
			}
			id := uo.SiaObject.ID
			if !now.Before(uo.PinBefore) {
				s.logger.Warn("upload expired before pinning; scheduling for re-upload",
					zap.Stringer("siaObjectID", &id),
					zap.Time("pinBefore", uo.PinBefore))
				if err := s.store.ScheduleObjectForReupload(id); err != nil && !errors.Is(err, objects.ErrObjectNotFound) {
					return fmt.Errorf("failed to schedule expired object for re-upload (siaObjectID=%v): %w", id, err)
				}
				continue
			}
			if err := s.pinObject(ctx, uo); err != nil {
				return err
			}
		}

		if len(toPin) < pinBatchSize {
			return nil
		}
	}
	return nil
}

func (s *Sia) reschedulePin(siaObjectID types.Hash256) error {
	next := time.Now().Add(pinRetryBackoff)
	if err := s.store.RescheduleUnpinnedObject(siaObjectID, next); err != nil && !errors.Is(err, objects.ErrObjectNotFound) {
		return fmt.Errorf("failed to reschedule unpinned object (siaObjectID=%v): %w", siaObjectID, err)
	}
	return nil
}

func (s *Sia) pinObject(ctx context.Context, uo objects.UnpinnedObject) error {
	id := uo.SiaObject.ID
	sdkObj, err := s.sdk.UnsealObject(uo.SiaObject.Sealed)
	if err != nil {
		s.logger.Error("failed to unseal object for pinning",
			zap.Stringer("siaObjectID", &id),
			zap.Error(err))
		return s.reschedulePin(id)
	}

	if err := s.sdk.PinObject(ctx, sdkObj); err != nil {
		s.logger.Warn("failed to pin object",
			zap.Stringer("siaObjectID", &id),
			zap.Error(err))
		return s.reschedulePin(id)
	}

	orphans, err := s.store.MarkObjectPinned(id)
	if err != nil {
		return fmt.Errorf("failed to mark object pinned (siaObjectID=%v): %w", id, err)
	}

	s.logger.Debug("object pinned", zap.Stringer("siaObjectID", &id))

	for _, o := range orphans {
		s.cleanupOrphan(filepath.Join(s.uploadDir(), o.Filename), o.Size)
	}
	return nil
}
