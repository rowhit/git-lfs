package commands

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"sync/atomic"

	"github.com/git-lfs/git-lfs/errors"
	"github.com/git-lfs/git-lfs/filepathfilter"
	"github.com/git-lfs/git-lfs/git"
	"github.com/git-lfs/git-lfs/lfs"
	"github.com/git-lfs/git-lfs/progress"
	"github.com/git-lfs/git-lfs/tq"
	"github.com/spf13/cobra"
)

const (
	// cleanFilterBufferCapacity is the desired capacity of the
	// `*git.PacketWriter`'s internal buffer when the filter protocol
	// dictates the "clean" command. 512 bytes is (in most cases) enough to
	// hold an entire LFS pointer in memory.
	cleanFilterBufferCapacity = 512

	// smudgeFilterBufferCapacity is the desired capacity of the
	// `*git.PacketWriter`'s internal buffer when the filter protocol
	// dictates the "smudge" command.
	smudgeFilterBufferCapacity = git.MaxPacketLength
)

// filterSmudgeSkip is a command-line flag owned by the `filter-process` command
// dictating whether or not to skip the smudging process, leaving pointers as-is
// in the working tree.
var filterSmudgeSkip bool

func filterCommand(cmd *cobra.Command, args []string) {
	requireStdin("This command should be run by the Git filter process")
	lfs.InstallHooks(false)

	s := git.NewFilterProcessScanner(os.Stdin, os.Stdout)

	if err := s.Init(); err != nil {
		ExitWithError(err)
	}

	caps, err := s.NegotiateCapabilities()
	if err != nil {
		ExitWithError(err)
	}

	var supportsDelay bool
	for _, cap := range caps {
		if cap == "capability=delay" {
			supportsDelay = true
			break
		}
	}

	available := make(map[string]*tq.Transfer)
	var closed uint32

	tq := tq.NewTransferQueue(tq.Download,
		getTransferManifest(), cfg.CurrentRemote,
		tq.WithProgress(progress.NewMeter(progress.WithOSEnv(cfg.Os))))
	wg := new(sync.WaitGroup)

	go func() {
		for t := range tq.Watch() {
			available[t.Name] = t
			wg.Done()
		}
	}()

	skip := filterSmudgeSkip || cfg.Os.Bool("GIT_LFS_SKIP_SMUDGE", false)
	filter := filepathfilter.New(cfg.FetchIncludePaths(), cfg.FetchExcludePaths())

	var malformed []string
	var malformedOnWindows []string

	for s.Scan() {
		var n int64
		var err error
		var w *git.PktlineWriter

		var delayed bool

		req := s.Request()

		switch req.Header["command"] {
		case "clean":
			s.WriteStatus(statusFromErr(nil))

			w = git.NewPktlineWriter(os.Stdout, cleanFilterBufferCapacity)
			err = clean(w, req.Payload, req.Header["pathname"], -1)
		case "smudge":
			w = git.NewPktlineWriter(os.Stdout, smudgeFilterBufferCapacity)

			if supportsDelay {
				if req.Header["can-delay"] == "1" {
					ptr, rest, err := lfs.DecodeFrom(req.Payload)
					if err != nil {
						if _, cerr := io.Copy(w, rest); cerr != nil {
							err = cerr
						}
						delayed = false
						break
					}

					path, err := lfs.LocalMediaPath(ptr.Oid)
					if err != nil {
						delayed = false
						break
					}

					wg.Add(1)
					tq.Add(req.Header["pathname"],
						path,
						ptr.Oid,
						ptr.Size)

					delayed = true
				} else {
					// When Git asks us again for an object
					// that was once delayed, it sends no
					// content. Discard the content so as to
					// advance the readerhead.
					io.Copy(ioutil.Discard, req.Payload)

					p, err := lfs.LocalMediaPath(available[req.Header["pathname"]].Oid)
					if err != nil {
						break
					}

					f, err := os.Open(p)
					if err != nil {
						break
					}

					n, err = io.Copy(w, f)
					f.Close()

					delete(available, req.Header["pathname"])

					s.WriteStatus(statusFromErr(nil))
				}
			} else {
				s.WriteStatus(statusFromErr(nil))
				n, err = smudge(w, req.Payload, req.Header["pathname"], skip, filter)
			}
		case "list_available_blobs":
			if atomic.CompareAndSwapUint32(&closed, 0, 1) {
				tq.Wait()
				wg.Wait()
			}

			s.WriteList(pathnames(available))
		default:
			ExitWithError(fmt.Errorf("Unknown command %q", req.Header["command"]))
		}

		if errors.IsNotAPointerError(err) {
			malformed = append(malformed, req.Header["pathname"])
			err = nil
		} else if possiblyMalformedSmudge(n) {
			malformedOnWindows = append(malformedOnWindows, req.Header["pathname"])
		}

		var status string
		if delayed {
			status = delayedStatusFromErr(err)
		} else {
			if ferr := w.Flush(); ferr != nil {
				status = statusFromErr(err)
			} else {
				status = statusFromErr(err)
			}
		}

		s.WriteStatus(status)
	}

	if len(malformed) > 0 {
		fmt.Fprintf(os.Stderr, "Encountered %d file(s) that should have been pointers, but weren't:\n", len(malformed))
		for _, m := range malformed {
			fmt.Fprintf(os.Stderr, "\t%s\n", m)
		}
	}

	if len(malformedOnWindows) > 0 {
		fmt.Fprintf(os.Stderr, "Encountered %d file(s) that may not have been copied correctly on Windows:\n")

		for _, m := range malformedOnWindows {
			fmt.Fprintf(os.Stderr, "\t%s\n", m)
		}

		fmt.Fprintf(os.Stderr, "\nSee: `git lfs help smudge` for more details.\n")
	}

	if err := s.Err(); err != nil && err != io.EOF {
		ExitWithError(err)
	}
}

func pathnames(available map[string]*tq.Transfer) []string {
	pathnames := make([]string, 0, len(available))
	for _, t := range available {
		pathnames = append(pathnames, fmt.Sprintf("pathname=%s", t.Name))
	}

	return pathnames
}

// statusFromErr returns the status code that should be sent over the filter
// protocol based on a given error, "err".
func statusFromErr(err error) string {
	if err != nil && err != io.EOF {
		return "error"
	}
	return "success"
}

func delayedStatusFromErr(err error) string {
	if err != nil && err != io.EOF {
		return "error"
	}
	return "delayed"
}

func init() {
	RegisterCommand("filter-process", filterCommand, func(cmd *cobra.Command) {
		cmd.Flags().BoolVarP(&filterSmudgeSkip, "skip", "s", false, "")
	})
}
