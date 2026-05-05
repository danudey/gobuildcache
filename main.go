package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/charmbracelet/log"
	"github.com/charmbracelet/x/term"
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"
	"golang.org/x/sync/singleflight"
)

type flagArray []string

func (v *flagArray) String() string {
	return fmt.Sprintf("%v", *v)
}

func (v *flagArray) Set(value string) error {
	*v = append(*v, value)
	return nil
}

type cmd string

const (
	cmdGet   = cmd("get")
	cmdPut   = cmd("put")
	cmdClose = cmd("close")
)

type request struct {
	ID       int64
	Command  cmd
	ActionID []byte    `json:",omitempty"`
	ObjectID []byte    `json:",omitempty"` // deprecated: use OutputID
	OutputID []byte    `json:",omitempty"`
	Body     io.Reader `json:"-"`
	BodySize int64     `json:",omitempty"`
}

type response struct {
	ID            int64
	Err           string     `json:",omitempty"`
	KnownCommands []cmd      `json:",omitempty"`
	Miss          bool       `json:",omitempty"`
	OutputID      []byte     `json:",omitempty"`
	Size          int64      `json:",omitempty"`
	Time          *time.Time `json:",omitempty"`
	DiskPath      string     `json:",omitempty"`
}

type Cacher struct {
	disk   *Disk
	bucket *Bucket
	flight singleflight.Group
}

func (c *Cacher) Get(ctx context.Context, req *request) (string, error) {
	actionID := hex.EncodeToString(req.ActionID)

	log.Debug("get", "action", actionID)

	outputID, err := c.bucket.OutputIDFromAction(ctx, actionID)
	if err != nil {
		return "", fmt.Errorf("getting output id from action (bucket): %w", err)
	}
	if outputID == "" {
		return "", nil
	}

	log.Debug("single flight get", "action", actionID, "output", outputID)
	pathname, err, shared := c.flight.Do("get"+outputID, func() (any, error) {
		return c.bucket.GetOutput(ctx, outputID)
	})
	log.Debug("single flight get done", "action", actionID, "output", outputID)

	if shared {
		log.Debug("get output shared", "output", outputID)
	}

	return pathname.(string), err
}

func (c *Cacher) Put(ctx context.Context, req *request) (string, error) {
	actionID := hex.EncodeToString(req.ActionID)
	outputID := hex.EncodeToString(req.OutputID)

	log.Debug("put", "action", actionID, "output", outputID)

	pathname, err, shared := c.flight.Do("put"+outputID, func() (any, error) {
		pathname, _, err := c.bucket.PutOutput(ctx, outputID, req.Body)
		return pathname, err
	})

	if shared {
		log.Debug("put output shared", "output", outputID)
	}

	if err != nil {
		return "", err
	}

	_, err = c.bucket.LinkActionToOutput(ctx, actionID, outputID)
	if err != nil {
		return pathname.(string), fmt.Errorf("linking action to output: %w", err)
	}

	return pathname.(string), err
}

func run(ctx context.Context, prefix, cachePath, bucketName string, readonly bool) error {
	// bucket, err := blob.OpenBucket(ctx, bucketURL)
	client, err := storage.NewGRPCClient(ctx)
	bucket := client.Bucket(bucketName)
	if err != nil {
		return fmt.Errorf("opening bucket: %w", err)
	}
	return serve(ctx, bucket, prefix, cachePath, readonly, os.Stdin, originalStdout)
}

func serve(ctx context.Context, bucket *storage.BucketHandle, prefix, cacheDir string, readonly bool, in io.Reader, out io.Writer) error {
	cacher := &Cacher{
		disk: &Disk{cacheDir: cacheDir},
	}
	cacher.bucket = &Bucket{disk: cacher.disk, prefix: prefix, bucket: bucket}
	cacher.bucket.Start(ctx)
	defer cacher.bucket.logStats()

	if err := os.MkdirAll(filepath.Join(cacher.disk.cacheDir, actionDir), 0o755); err != nil {
		return fmt.Errorf("creating cache action dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(cacher.disk.cacheDir, outputDir), 0o755); err != nil {
		return fmt.Errorf("creating cache output dir: %w", err)
	}

	caps := []cmd{cmdClose, cmdGet}
	if !readonly {
		caps = append(caps, cmdPut)
	}

	r, w := bufio.NewReader(in), bufio.NewWriter(out)
	dec, enc := json.NewDecoder(r), json.NewEncoder(w)

	if err := enc.Encode(response{KnownCommands: caps}); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}

	var mu sync.Mutex
	for {
		var req request
		if err := dec.Decode(&req); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		// handle accidental naming of OutputID prior to Go 1.24
		if req.ObjectID != nil {
			req.OutputID = req.ObjectID
		}

		if req.Command == cmdPut {
			if req.BodySize > 0 {
				var buf []byte
				if err := dec.Decode(&buf); err != nil {
					return fmt.Errorf("decoding body: %w", err)
				}
				if int64(len(buf)) != req.BodySize {
					return fmt.Errorf("incorrect length: %d != %d", len(buf), req.BodySize)
				}
				req.Body = bytes.NewReader(buf)
			} else {
				req.Body = bytes.NewReader(nil)
			}
		}

		go func() {
			resp := handleRequest(ctx, cacher, &req)
			mu.Lock()
			enc.Encode(resp)
			w.Flush()
			mu.Unlock()
		}()
	}
}

func handleRequest(ctx context.Context, c *Cacher, req *request) *response {
	resp := &response{ID: req.ID}

	var err error
	switch req.Command {
	case cmdClose:
		c.bucket.Close()

	case cmdGet:
		now := time.Now()
		resp.DiskPath, err = c.Get(ctx, req)
		if err != nil {
			log.Error("get", "action", hex.EncodeToString(req.ActionID), "output", resp.DiskPath, "err", err, "took", time.Since(now))
			resp.Err = err.Error()
		} else {
			log.Debug("get", "action", hex.EncodeToString(req.ActionID), "output", resp.DiskPath, "took", time.Since(now))
		}
		if resp.DiskPath == "" {
			resp.Miss = true
		}

	case cmdPut:
		now := time.Now()
		resp.DiskPath, err = c.Put(ctx, req)
		if err != nil {
			log.Error("put", "action", hex.EncodeToString(req.ActionID), "output", resp.DiskPath, "err", err, "took", time.Since(now))
			resp.Err = err.Error()
		} else {
			log.Debug("put", "action", hex.EncodeToString(req.ActionID), "output", resp.DiskPath, "took", time.Since(now))
		}
	}

	populateFileInfo(req, resp)
	return resp
}

// populateFileInfo fills Size/Time/OutputID from the on-disk artifact named by
// resp.DiskPath. Skipped for cmdClose (no disk path) and on cache miss
// (empty DiskPath would otherwise produce a spurious stat error).
func populateFileInfo(req *request, resp *response) {
	if req.Command == cmdClose || resp.DiskPath == "" {
		return
	}
	fi, err := os.Stat(resp.DiskPath)
	if err != nil {
		resp.Err = err.Error()
		return
	}
	resp.OutputID, err = hex.DecodeString(filepath.Base(resp.DiskPath))
	if err != nil {
		resp.Err = "invalid output id"
	}
	resp.Size = fi.Size()
	modTime := fi.ModTime()
	resp.Time = &modTime
}

var originalStdout = os.Stdout

func init() {
	// annoyingly, gocloud.dev prints to stdout messing with the expected JSON output
	os.Stdout = os.Stderr
}

func main() {
	var prefix string
	var cachePath string
	var cpuProfile string
	var verbose bool
	var readonly bool
	var envmap flagArray

	flag.StringVar(&prefix, "p", "", "prefix")
	flag.StringVar(&cachePath, "c", "", "cache-dir")
	flag.StringVar(&cpuProfile, "cpuprofile", "", "write cpu profile to file")
	flag.BoolVar(&verbose, "v", false, "verbose")
	flag.BoolVar(&readonly, "readonly", false, "readonly")
	flag.Var(&envmap, "env", "remap environment variable (example: GOOGLE_APPLICATION_CREDENTIALS=MY_ENV)")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "%s <bucket url>\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	if cachePath == "" {
		cacheDir, err := os.UserCacheDir()
		if err != nil {
			log.Error("failed to detect user's cache dir", "err", err)
			os.Exit(1)
		}
		cachePath = filepath.Join(cacheDir, ".gocachebucket")
	}

	for _, env := range envmap {
		key, val, ok := strings.Cut(env, "=")
		if !ok {
			continue
		}
		os.Setenv(key, os.Getenv(val))
	}

	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(1)
	}

	level := log.InfoLevel
	if verbose {
		level = log.DebugLevel
	}
	log.SetLevel(level)

	if !term.IsTerminal(os.Stderr.Fd()) {
		log.SetFormatter(log.LogfmtFormatter)
	}

	log.Info("Using cache dir", "cachePath", cachePath)
	log.Info("Using gcp bucket", "bucket", flag.Arg(0))

	if cpuProfile != "" {
		f, err := os.Create(cpuProfile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
		log.Info("Saving CPU profile", "cpuProfile", cpuProfile)

	}

	if err := run(context.Background(), prefix, cachePath, flag.Arg(0), readonly); err != nil {
		log.Error("run error", "err", err)
		os.Exit(1)
	}
}
