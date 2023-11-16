// Command snappr prunes time-based snapshots from stdin.
package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pgaskin/snappr"
	"github.com/spf13/pflag"
)

func main() {
	if status := Main(os.Args, os.Stdin, os.Stdout, os.Stderr); status != 0 {
		os.Exit(status)
	}
}

type timezoneFlag struct {
	loc *time.Location
}

func pflag_TimezoneP(opt *pflag.FlagSet, name, shorthand string, value *time.Location, usage string) **time.Location {
	f := &timezoneFlag{value}
	opt.VarP(f, name, shorthand, usage)
	return &f.loc
}

func (t *timezoneFlag) Type() string {
	return "tz"
}

func (t *timezoneFlag) String() string {
	if t.loc == nil {
		return ""
	}
	return t.loc.String()
}

func (t *timezoneFlag) Set(s string) error {
	switch string(s) {
	case "":
		t.loc = nil
	case "UTC", "utc":
		t.loc = time.UTC
	case "Local", "local":
		t.loc = time.Local
	default:
		loc, err := time.LoadLocation(s)
		if err != nil {
			return err
		}
		t.loc = loc
	}
	return nil
}

func Main(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	opt := pflag.NewFlagSet(args[0], pflag.ContinueOnError)
	var (
		Quiet     = opt.BoolP("quiet", "q", false, "do not show warnings about invalid or unmatched input lines")
		Extract   = opt.StringP("extract", "e", "", "extract the timestamp from each input line using the provided regexp, which must contain up to one capture group")
		Extended  = opt.BoolP("extended-regexp", "E", false, "use full regexp syntax rather than POSIX (see pkg.go.dev/regexp/syntax)")
		Only      = opt.BoolP("only", "o", false, "only print the part of the line matching the regexp")
		Parse     = opt.StringP("parse", "p", "", "parse the timestamp using the specified Go time format (see pkg.go.dev/time#pkg-constants and the examples below) rather than a unix timestamp")
		ParseIn   = pflag_TimezoneP(opt, "parse-timezone", "Z", nil, "use a specific timezone rather than whatever is set for --timezone if no timezone is parsed from the timestamp itself")
		In        = pflag_TimezoneP(opt, "timezone", "z", time.UTC, "convert all timestamps to this timezone while pruning snapshots (use \"local\" for the default system timezone)")
		Invert    = opt.BoolP("invert", "v", false, "output the snapshots to keep instead of the ones to prune")
		Why       = opt.BoolP("why", "w", false, "explain why each snapshot is being kept to stderr")
		Summarize = opt.BoolP("summarize", "s", false, "summarize retention policy results to stderr")
		Help      = opt.BoolP("help", "h", false, "show this help text")
	)
	if err := opt.Parse(args[1:]); err != nil {
		fmt.Fprintf(stderr, "snappr: fatal: %v\n", err)
		return 2
	}

	if *Help {
		fmt.Fprintf(stdout, "usage: %s [options] policy...\n", args[0])
		fmt.Fprintf(stdout, "\noptions:\n%s", opt.FlagUsages())
		fmt.Fprintf(stdout, "\ntime format examples:\n")
		fmt.Fprintf(stdout, "  - Mon Jan 02 15:04:05 2006\n")
		fmt.Fprintf(stdout, "  - 02 Jan 06 15:04 MST\n")
		fmt.Fprintf(stdout, "  - 2006-01-02T15:04:05Z07:00\n")
		fmt.Fprintf(stdout, "  - 2006-01-02T15:04:05\n")
		fmt.Fprintf(stdout, "\npolicy: N@unit:X\n")
		fmt.Fprintf(stdout, "  - keep the last N snapshots every X units\n")
		fmt.Fprintf(stdout, "  - omit the N@ to keep an infinite number of snapshots\n")
		fmt.Fprintf(stdout, "  - if :X is omitted, it defaults to :1\n")
		fmt.Fprintf(stdout, "  - there may only be one N specified for each unit:X pair\n")
		fmt.Fprintf(stdout, "\nunit:\n")
		fmt.Fprintf(stdout, "  last       snapshot count (X must be 1)\n")
		fmt.Fprintf(stdout, "  secondly   clock seconds (can also use the format #h#m#s, omitting any zeroed units)\n")
		fmt.Fprintf(stdout, "  daily      calendar days\n")
		fmt.Fprintf(stdout, "  monthly    calendar months\n")
		fmt.Fprintf(stdout, "  yearly     calendar years\n")
		fmt.Fprintf(stdout, "\nnotes:\n")
		fmt.Fprintf(stdout, "  - output lines consist of filtered input lines\n")
		fmt.Fprintf(stdout, "  - input is read from stdin, and should consist of unix timestamps (or more if --extract and/or --parse are set)\n")
		fmt.Fprintf(stdout, "  - invalid/unmatched input lines are ignored, or passed through if --invert is set (and a warning is printed unless --quiet is set)\n")
		fmt.Fprintf(stdout, "  - everything will still work correctly even if timezones are different\n")
		fmt.Fprintf(stdout, "  - snapshots are always ordered by their real (i.e., UTC) time\n")
		fmt.Fprintf(stdout, "  - if using --parse-in, beware of duplicate timestamps at DST transitions (if the offset isn't included whatever you use as the\n")
		fmt.Fprintf(stdout, "    snapshot name, and your timezone has DST, you may end up with two snapshots for different times with the same name.\n")
		fmt.Fprintf(stdout, "  - timezones will only affect the exact point at which calendar days/months/years are split\n")
		if !*Help {
			return 2
		}
		return 0
	}

	if opt.NArg() < 1 {
		fmt.Fprintf(stderr, "snappr: fatal: at least one policy must be specified (see --help)\n")
		return 2
	}

	if *In == nil {
		fmt.Fprintf(stderr, "snappr: fatal: timezone must not be empty\n")
		return 2
	}

	if *ParseIn == nil {
		*ParseIn = *In
	}

	policy, err := snappr.ParsePolicy(opt.Args()...)
	if err != nil {
		fmt.Fprintf(stderr, "snappr: fatal: invalid policy: %v\n", err)
		return 2
	}

	var extract *regexp.Regexp
	if *Extract != "" {
		var err error
		if *Extended {
			extract, err = regexp.Compile(*Extract)
		} else {
			extract, err = regexp.CompilePOSIX(*Extract)
		}
		if err == nil && extract.NumSubexp() > 1 {
			err = fmt.Errorf("must contain no more than one capture group")
		}
		if err != nil {
			fmt.Fprintf(stderr, "snappr: fatal: --extract regexp is invalid: %v\n", err)
			return 2
		}
	}

	times, lines, err := func() (times []time.Time, lines []string, err error) {
		sc := bufio.NewScanner(stdin)
		for sc.Scan() {
			line := sc.Text()
			if len(line) == 0 {
				continue
			}

			var bad bool

			var ts string
			if extract == nil {
				ts = strings.TrimSpace(line)
			} else {
				if m := extract.FindStringSubmatch(line); m == nil {
					if !*Quiet {
						fmt.Fprintf(stderr, "snappr: warning: failed extract timestamp from %q using regexp %q\n", line, extract.String())
						bad = true
					}
				} else {
					if *Only {
						line = m[0]
					}
					ts = m[len(m)-1]
				}
			}

			var t time.Time
			if !bad {
				if *Parse == "" {
					if n, err := strconv.ParseInt(ts, 10, 64); err != nil {
						if !*Quiet {
							fmt.Fprintf(stderr, "snappr: warning: failed to parse unix timestamp %q: %v\n", ts, err)
						}
						bad = true
					} else {
						t = time.Unix(n, 0)
					}
				} else {
					if v, err := time.ParseInLocation(*Parse, ts, *ParseIn); err != nil {
						if !*Quiet {
							fmt.Fprintf(stderr, "snappr: warning: failed to parse timestamp %q using layout %q: %v\n", ts, *Parse, err)
						}
						bad = true
					} else {
						t = v
					}
				}
				t = t.In(*In)
			}

			if bad {
				times = append(times, time.Time{})
			} else {
				times = append(times, t)
			}
			lines = append(lines, line)
		}
		return times, lines, sc.Err()
	}()
	if err != nil {
		fmt.Fprintf(stderr, "snappr: fatal: failed to read stdin: %v\n", err)
		return 1
	}

	snapshots := make([]time.Time, 0, len(times))
	snapshotMap := make([]int, 0, len(times))
	for i, t := range times {
		if !t.IsZero() {
			snapshots = append(snapshots, t)
			snapshotMap = append(snapshotMap, i)
		}
	}

	keep, need := snappr.Prune(snapshots, policy, *In)

	discard := make([]bool, len(times))
	for at, why := range keep {
		discard[snapshotMap[at]] = len(why) == 0
	}
	for i, x := range discard {
		if *Invert {
			if x {
				continue
			}
		} else {
			if !x {
				continue
			}
		}
		fmt.Fprintln(stdout, lines[i])
	}

	var pruned int
	ndig := digits(len(keep))
	for at, why := range keep {
		if len(why) != 0 {
			ps := make([]string, len(why))
			for i, period := range why {
				ps[i] = period.String()
			}
			if *Why {
				fmt.Fprintf(stderr, "snappr: why: keep [%*d/%*d] %s :: %s\n", ndig, at+1, ndig, len(keep), snapshots[at].Format("Mon 2006 Jan _2 15:04:05"), strings.Join(ps, ", "))
			}
		} else {
			pruned++
		}
	}
	if *Summarize {
		var cmax int
		policy.Each(func(_ snappr.Period, count int) {
			cmax = max(cmax, count)
		})
		cdig := digits(cmax)
		need.Each(func(period snappr.Period, count int) {
			if count < 0 {
				fmt.Fprintf(stderr, "snappr: summary: (%s) %s\n", strings.Repeat("*", cdig), period)
			} else if count == 0 {
				fmt.Fprintf(stderr, "snappr: summary: (%*d) %s\n", cdig, policy.Get(period), period)
			} else {
				fmt.Fprintf(stderr, "snappr: summary: (%*d) %s (missing %d)\n", cdig, policy.Get(period), period, count)
			}
		})
		fmt.Fprintf(stderr, "snappr: summary: pruning %d/%d snapshots\n", pruned, len(keep))
	}
	return 0
}

func digits(n int) int {
	if n == 0 {
		return 1
	}
	count := 0
	for n != 0 {
		n /= 10
		count++
	}
	return count
}
