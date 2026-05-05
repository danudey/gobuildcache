package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/charmbracelet/log"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
)

const (
	actionDir = "action"
	outputDir = "output"

	// How long a recorded "this action has no output in the bucket" marker
	// suppresses re-checking. Long enough to prevent hammering during a single
	// build, short enough that a freshly-uploaded entry becomes visible soon.
	emptyMarkerTTL = 10 * time.Minute
)

// isValidID reports whether s is safe to use as a cache ID embedded in a
// filesystem path. IDs we generate are lowercase hex from hex.EncodeToString;
// values arriving from untrusted sources (bucket metadata, on-disk symlink
// targets) must be rejected before they can drive path traversal.
func isValidID(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

type OutputInfo struct {
	ID   string
	Path string
	Size int64
	Time int64
}

type Storage interface {
	PutOutput(ctx context.Context, outputID string, r io.Reader) (string, bool, error)
	GetOutput(ctx context.Context, outputID string) (string, error)

	OutputIDFromAction(ctx context.Context, actionID string) (string, error)
	LinkActionToOutput(ctx context.Context, actionID, outputID string) error
}

type Disk struct {
	cacheDir string
}

type Bucket struct {
	disk   *Disk
	bucket *blob.Bucket
	jobs   chan string
	wg     sync.WaitGroup

	closeOnce sync.Once

	stats cacheStats
}

type cacheStats struct {
	actionDiskHits        atomic.Int64
	actionDiskRequests    atomic.Int64
	actionBucketHits      atomic.Int64
	actionBucketRequests  atomic.Int64
	outputDiskHits        atomic.Int64
	outputDiskRequests    atomic.Int64
	outputBucketHits      atomic.Int64
	outputBucketRequests  atomic.Int64
	outputBucketTotalNano atomic.Int64
}

func (c *cacheStats) actionDiskPercent() float32 {
	if c.actionDiskRequests.Load() == 0 {
		return float32(0)
	}
	return float32(c.actionDiskHits.Load()) / float32(c.actionDiskRequests.Load()) * 100
}

func (c *cacheStats) actionBucketPercent() float32 {
	if c.actionBucketRequests.Load() == 0 {
		return float32(0)
	}
	return float32(c.actionBucketHits.Load()) / float32(c.actionBucketRequests.Load()) * 100
}

func (c *cacheStats) outputDiskPercent() float32 {
	if c.outputDiskRequests.Load() == 0 {
		return float32(0)
	}
	return float32(c.outputDiskHits.Load()) / float32(c.outputDiskRequests.Load()) * 100
}

func (c *cacheStats) outputBucketPercent() float32 {
	if c.outputBucketRequests.Load() == 0 {
		return float32(0)
	}
	return float32(c.outputBucketHits.Load()) / float32(c.outputBucketRequests.Load()) * 100
}

func (c *cacheStats) actionDiskStats() string {
	return fmt.Sprintf("%d/%d (%6.2f%%)",
		c.actionDiskHits.Load(),
		c.actionDiskRequests.Load(),
		c.actionDiskPercent(),
	)
}

func (c *cacheStats) actionBucketStats() string {
	return fmt.Sprintf("%d/%d (%6.2f%%)",
		c.actionBucketHits.Load(),
		c.actionBucketRequests.Load(),
		c.actionBucketPercent(),
	)
}

func (c *cacheStats) outputDiskStats() string {
	return fmt.Sprintf("%d/%d (%6.2f%%)",
		c.outputDiskHits.Load(),
		c.outputDiskRequests.Load(),
		c.outputDiskPercent(),
	)
}
func (c *cacheStats) outputBucketStats() string {
	return fmt.Sprintf("%d/%d (%6.2f%%)",
		c.outputBucketHits.Load(),
		c.outputBucketRequests.Load(),
		c.outputBucketPercent(),
	)
}

func (c *cacheStats) outputBucketLatency() string {
	return fmt.Sprintf("total: %s average: %s",
		c.outputBucketTotalDuration(),
		c.outputBucketAverageDuration(),
	)
}

func (c *cacheStats) outputBucketTotalDuration() time.Duration {
	return time.Duration(c.outputBucketTotalNano.Load())
}

func (c *cacheStats) outputBucketAverageDuration() time.Duration {
	requests := c.outputBucketRequests.Load()
	if requests == 0 {
		return 0
	}
	return time.Duration(c.outputBucketTotalNano.Load() / requests)
}

func (b *Bucket) logStats() {
	logFmt := "%15s %20s"
	log.Infof(logFmt, "action_disk", b.stats.actionDiskStats())
	log.Infof(logFmt, "action_bucket", b.stats.actionBucketStats())
	log.Infof(logFmt, "output_disk", b.stats.outputDiskStats())
	log.Infof(logFmt, "output_bucket", b.stats.outputBucketStats())
	log.Infof(logFmt, "latency", b.stats.outputBucketLatency())
}

func (d *Disk) PutOutput(ctx context.Context, outputID string, r io.Reader) (string, bool, error) {
	outputPathname := filepath.Join(d.cacheDir, outputDir, outputID)

	// do nothing if already exists
	if _, err := os.Stat(outputPathname); err == nil {
		return outputPathname, true, nil
	}

	log.Debug("persisting to disk", "path", outputPathname)

	f, err := os.CreateTemp(d.cacheDir, "output")
	if err != nil {
		return "", false, fmt.Errorf("creating temporary output file: %w", err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	_, err = io.Copy(f, r)
	if err != nil {
		return "", false, fmt.Errorf("copying output to disk: %w", err)
	}

	if err := f.Close(); err != nil {
		return "", false, fmt.Errorf("flushing output to disk: %w", err)
	}

	if err := os.Rename(f.Name(), outputPathname); err != nil {
		return "", false, fmt.Errorf("renaming: %w", err)
	}

	return outputPathname, false, nil
}

func (d *Disk) GetOutput(ctx context.Context, outputID string) (string, error) {
	return filepath.Join(d.cacheDir, outputDir, outputID), nil
}

func (d *Disk) OutputIDFromAction(ctx context.Context, actionID string) (string, error) {
	actionPathname := filepath.Join(d.cacheDir, actionDir, actionID)

	outputPathname, err := os.Readlink(actionPathname)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}

	outputID := filepath.Base(outputPathname)
	if !isValidID(outputID) {
		return "", fmt.Errorf("invalid output id %q in action symlink %s", outputID, actionPathname)
	}
	return outputID, nil
}

func (d *Disk) LinkActionToOutput(ctx context.Context, actionID, outputID string) (bool, error) {
	actionPathname := filepath.Join(d.cacheDir, actionDir, actionID)
	outputPathname := filepath.Join("..", outputDir, outputID)

	// Check if existing symlink already points to the correct output
	existing, err := os.Readlink(actionPathname)
	if err == nil && existing == outputPathname {
		return true, nil
	}

	// Atomically create/replace symlink by creating at temp path then renaming
	tmpPathname := fmt.Sprintf("%s.tmp.%x", actionPathname, rand.Uint64())
	// Conceivably the temporary filename could already exist and this would
	// error, but it seems unlikely enough to not worry about.
	if err := os.Symlink(outputPathname, tmpPathname); err != nil {
		return false, err
	}
	if err := os.Rename(tmpPathname, actionPathname); err != nil {
		os.Remove(tmpPathname)
		return false, err
	}
	return false, nil
}

func (b *Bucket) OutputIDFromAction(ctx context.Context, actionID string) (string, error) {
	outputID, err := b.disk.OutputIDFromAction(ctx, actionID)
	if err != nil {
		return "", fmt.Errorf("output id from action (disk): %w", err)
	}

	b.stats.actionDiskRequests.Add(1)
	if outputID != "" {
		b.stats.actionDiskHits.Add(1)
		log.Debug("returning output id", "action", actionID, "output", outputID)
		return outputID, nil
	}

	// TODO: come up with a better solution for this scenario
	// If we fetch from remote storage and there's nothing there, we store an "empty" link,
	// just so that we don't keep trying to fetch this (it adds latency, only to find nothing).
	// The downside is that if at some point it does exist in remote storage, we might not
	// immediately observe that.
	cacheEmptyOutputPath := filepath.Join(b.disk.cacheDir, actionDir, actionID+".empty")
	b.stats.actionBucketRequests.Add(1)
	if fi, err := os.Stat(cacheEmptyOutputPath); err == nil {
		if time.Since(fi.ModTime()) < emptyMarkerTTL {
			log.Debug("empty found", "action", actionID, "output", outputID)
			return "", nil
		}
		log.Debug("empty marker expired", "action", actionID)
	}

	attr, err := b.bucket.Attributes(ctx, path.Join(actionDir, actionID))
	log.Debug("fetched attributes", "action", actionID, "output", outputID, "err", err)
	if gcerrors.Code(err) == gcerrors.NotFound {
		log.Debug("created found", "action", actionID, "output", outputID)
		if err := os.WriteFile(cacheEmptyOutputPath, nil, 0o600); err != nil {
			log.Warn("writing empty marker", "action", actionID, "err", err)
		}
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("attribute for %v: %w", actionID, err)
	}

	outputID = attr.Metadata["output_id"]
	if outputID == "" {
		log.Debug("no metadata output id", "action", actionID, "output", outputID)
		return "", nil
	}
	if !isValidID(outputID) {
		return "", fmt.Errorf("invalid output_id %q in bucket metadata for action %s", outputID, actionID)
	}
	b.stats.actionBucketHits.Add(1)

	log.Debug("linking action to output from output from action", "action", actionID, "output", outputID)
	if _, err := b.disk.LinkActionToOutput(ctx, actionID, outputID); err != nil {
		log.Warn("linking action to output", "action", actionID, "output", outputID, "err", err)
	}

	return outputID, nil
}

func (b *Bucket) LinkActionToOutput(ctx context.Context, actionID, outputID string) (bool, error) {
	exists, err := b.disk.LinkActionToOutput(ctx, actionID, outputID)
	if err != nil || exists {
		return exists, err
	}

	return false, b.bucket.Upload(ctx, path.Join(actionDir, actionID), bytes.NewReader(nil), &blob.WriterOptions{
		Metadata:    map[string]string{"output_id": outputID},
		ContentType: "text/plain",
	})
}

func (b *Bucket) PutOutput(ctx context.Context, outputID string, r io.Reader) (string, bool, error) {
	pathname, exists, err := b.disk.PutOutput(ctx, outputID, r)
	if err != nil {
		return "", false, err
	}
	if exists {
		return pathname, true, nil
	}

	log.Debug("scheduling upload", "path", pathname)
	if err := b.enqueueUpload(pathname); err != nil {
		return pathname, false, err
	}

	return pathname, false, nil
}

// enqueueUpload sends to the job channel, recovering if a concurrent Close
// has shut it down. A protocol-conformant driver issues no puts after close,
// but a malformed one would otherwise panic the process.
func (b *Bucket) enqueueUpload(pathname string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("upload queue closed: %v", r)
		}
	}()
	b.jobs <- pathname
	return nil
}

func (b *Bucket) Start(ctx context.Context) {
	// queue up to 1000
	b.jobs = make(chan string, 1000)

	// 20 workers ought to be enough for anybody
	for range 20 {
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()

			for pathname := range b.jobs {
				f, err := os.Open(pathname)
				if err != nil {
					log.Error("opening file for upload", "path", pathname, "err", err)
					continue
				}

				now := time.Now()
				err = b.bucket.Upload(ctx, path.Join(outputDir, filepath.Base(pathname)), f, &blob.WriterOptions{ContentType: "application/octet-stream"})
				f.Close()
				if err != nil {
					log.Error("uploading file", "path", pathname, "err", err, "took", time.Since(now))
				} else {
					log.Debug("uploaded file", "path", pathname, "took", time.Since(now))
				}
			}
		}()
	}
}

func (b *Bucket) Close() {
	b.closeOnce.Do(func() {
		log.Debug("waiting for uploads...")

		now := time.Now()
		close(b.jobs)
		b.wg.Wait()

		log.Debug("waited for uploads", "took", time.Since(now))
	})
}

func (b *Bucket) GetOutput(ctx context.Context, outputID string) (string, error) {
	log.Debug("getting output from disk", "output", outputID)
	b.stats.outputDiskRequests.Add(1)

	pathname, err := b.disk.GetOutput(ctx, outputID)
	if err != nil {
		return "", err
	}

	log.Debug("got output from disk", "output", outputID, "path", pathname, "err", err)

	if _, err := os.Stat(pathname); err == nil {
		b.stats.outputDiskHits.Add(1)
		log.Debug("returning pathname", "output", outputID, "path", pathname)

		return pathname, nil
	}

	log.Debug("downloading", "output", outputID)
	b.stats.outputBucketRequests.Add(1)

	fetchStart := time.Now()
	rdr, err := b.bucket.NewReader(ctx, path.Join(outputDir, outputID), nil)
	if gcerrors.Code(err) == gcerrors.NotFound {
		b.stats.outputBucketTotalNano.Add(time.Since(fetchStart).Nanoseconds())
		return "", nil
	}
	if err != nil {
		b.stats.outputBucketTotalNano.Add(time.Since(fetchStart).Nanoseconds())
		return "", err
	}
	defer rdr.Close()

	f, err := os.CreateTemp(b.disk.cacheDir, "output")
	if err != nil {
		return "", fmt.Errorf("creating temporary output file: %w", err)
	}
	keep := false
	defer func() {
		f.Close()
		if !keep {
			os.Remove(f.Name())
		}
	}()

	// outputID is the SHA256 of the cached content (per Go's cache protocol).
	// Hash while streaming so a poisoned bucket can't feed mismatched bytes
	// into the build.
	h := sha256.New()
	size, err := io.Copy(io.MultiWriter(f, h), rdr)
	b.stats.outputBucketTotalNano.Add(time.Since(fetchStart).Nanoseconds())
	if err != nil {
		return "", fmt.Errorf("downloading output: %w", err)
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("flushing output: %w", err)
	}

	if got := hex.EncodeToString(h.Sum(nil)); got != outputID {
		return "", fmt.Errorf("output %s hash mismatch: got %s", outputID, got)
	}

	if err := os.Rename(f.Name(), pathname); err != nil {
		return "", fmt.Errorf("renaming output: %w", err)
	}
	keep = true
	b.stats.outputBucketHits.Add(1)

	log.Debug("downloaded to disk", "output", outputID, "size", size)

	return pathname, nil
}
