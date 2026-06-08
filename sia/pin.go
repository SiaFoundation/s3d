package sia

import (
	"context"
	"errors"
	"path/filepath"
	"time"

	"github.com/SiaFoundation/s3d/sia/objects"
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
		s.performObjectPinning(ctx)
		if ctx.Err() != nil {
			return
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

// performObjectPinning fetches due unpinned objects in batches and pins them. On a
// per-object failure the row's next_attempt_at is bumped so the loop will
// retry it later instead of blocking the rest of the queue. Rows whose
// pin_before deadline has passed are scheduled for re-upload instead.
func (s *Sia) performObjectPinning(ctx context.Context) {
	for ctx.Err() == nil {
		now := time.Now()
		toPin, err := s.store.ObjectsForPinning(now, pinBatchSize)
		if err != nil {
			s.logger.Error("failed to fetch unpinned objects", zap.Error(err))
			return
		} else if len(toPin) == 0 {
			return
		}

		for _, uo := range toPin {
			if ctx.Err() != nil {
				return
			}
			if !now.Before(uo.PinBefore) {
				s.logger.Warn("upload expired before pinning; scheduling for re-upload",
					zap.String("bucket", uo.Bucket),
					zap.String("name", uo.Name),
					zap.Time("pinBefore", uo.PinBefore))
				if err := s.store.ScheduleObjectForReupload(uo.Bucket, uo.Name); err != nil && !errors.Is(err, objects.ErrObjectNotFound) {
					s.logger.Error("failed to schedule expired object for re-upload",
						zap.String("bucket", uo.Bucket),
						zap.String("name", uo.Name),
						zap.Error(err))
				}
				continue
			}
			s.pinObject(ctx, uo)
		}

		if len(toPin) < pinBatchSize {
			return
		}
	}
}

func (s *Sia) pinObject(ctx context.Context, uo objects.UnpinnedObject) {
	reschedulePin := func(uo objects.UnpinnedObject) {
		next := time.Now().Add(pinRetryBackoff)
		if err := s.store.RescheduleUnpinnedObject(uo.Bucket, uo.Name, next); err != nil && !errors.Is(err, objects.ErrObjectNotFound) {
			s.logger.Error("failed to reschedule unpinned object",
				zap.String("bucket", uo.Bucket),
				zap.String("name", uo.Name),
				zap.Error(err))
		}
	}

	sdkObj, err := s.sdk.UnsealObject(uo.SiaObject.Sealed)
	if err != nil {
		s.logger.Error("failed to unseal object for pinning",
			zap.String("bucket", uo.Bucket),
			zap.String("name", uo.Name),
			zap.Error(err))
		reschedulePin(uo)
		return
	}

	if err := s.sdk.PinObject(ctx, sdkObj); err != nil {
		s.failedUploads.Add(1)
		s.logger.Warn("failed to pin object",
			zap.String("bucket", uo.Bucket),
			zap.String("name", uo.Name),
			zap.Error(err))
		reschedulePin(uo)
		return
	}

	orphanFile, orphanSize, err := s.store.MarkObjectPinned(uo.Bucket, uo.Name)
	if err != nil {
		s.logger.Error("failed to mark object pinned",
			zap.String("bucket", uo.Bucket),
			zap.String("name", uo.Name),
			zap.Error(err))
		return
	}

	s.logger.Debug("object pinned",
		zap.String("bucket", uo.Bucket),
		zap.String("name", uo.Name))

	if orphanFile != "" {
		s.cleanupOrphan(filepath.Join(s.uploadDir(), orphanFile), orphanSize)
	}
}
