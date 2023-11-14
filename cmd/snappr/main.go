// Command snappr
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

var (
	Quiet     = pflag.BoolP("quiet", "q", false, "do not show warnings about invalid or unmatched input lines")
	Extract   = pflag.StringP("extract", "e", "", "extract the timestamp from each input line using the provided regexp, which must contain up to one capture group")
	Extended  = pflag.BoolP("extended-regexp", "E", false, "use full regexp syntax rather than POSIX (see pkg.go.dev/regexp/syntax)")
	Only      = pflag.BoolP("only", "o", false, "only print the part of the line matching the regexp")
	Parse     = pflag.StringP("parse", "p", "", "parse the timestamp using the specified Go time format (see pkg.go.dev/time#pkg-constants) rather than a unix timestamp")
	Local     = pflag.BoolP("local-time", "L", false, "use the default timezone rather than UTC if no timezone is parsed from the timestamp")
	Invert    = pflag.BoolP("invert", "v", false, "output the snapshots to keep instead of the ones to prune")
	Why       = pflag.BoolP("why", "w", false, "explain why each snapshot is being kept to stderr")
	Summarize = pflag.BoolP("summarize", "s", false, "summarize retention policy results to stderr")
	Help      = pflag.BoolP("help", "h", false, "show this help text")
)

func main() {
	pflag.Parse()

	if pflag.NArg() < 1 || *Help {
		fmt.Printf("usage: %s [options] policy...\n", os.Args[0])
		fmt.Printf("\noptions:\n%s", pflag.CommandLine.FlagUsages())
		fmt.Printf("\npolicy: N@unit:X\n")
		fmt.Printf("  - keep the last N snapshots every X units\n")
		fmt.Printf("  - omit the N@ to keep an infinite number of snapshots\n")
		fmt.Printf("  - if :X is omitted, it defaults to :1\n")
		fmt.Printf("  - there may only be one N specified for each unit:X pair\n")
		fmt.Printf("\nunit:\n")
		fmt.Printf("  last       snapshot count\n")
		fmt.Printf("  secondly   clock seconds (can also use the format #h#m#s, omitting any zeroed units)\n")
		fmt.Printf("  daily      calendar days\n")
		fmt.Printf("  monthly    calendar months\n")
		fmt.Printf("  yearly     calendar years\n")
		fmt.Printf("\nnotes:\n")
		fmt.Printf("  - output lines consist of filtered input lines\n")
		fmt.Printf("  - input is read from stdin, and should consist of unix timestamps (or more if --extract and/or --parse are set)\n")
		fmt.Printf("  - invalid/unmatched input lines are ignored, or passed through if --invert is set (and a warning is printed unless --quiet is set)\n")
		fmt.Printf("  - everything will still work correctly even if timezones are different\n")
		fmt.Printf("  - snapshots are ordered by their UTC time\n")
		fmt.Printf("  - timezones will only affect the exact point at which calendar days/months/years are split\n")
		if *Help {
			os.Exit(0)
		} else {
			os.Exit(2)
		}
	}

	policy, err := parse(pflag.Args()...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "snappr: fatal: invalid policy: %v\n", err)
		os.Exit(2)
	}

	var extract *regexp.Regexp
	if *Extract != "" {
		var err error
		if *Extended {
			extract, err = regexp.Compile(*Extract)
		} else {
			extract, err = regexp.CompilePOSIX(*Extract)
		}
		if err == nil && extract.NumSubexp() > 2 {
			err = fmt.Errorf("must contain up to one capture group")
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "snappr: fatal: --extract regexp is invalid: %v\n", err)
			os.Exit(2)
		}
	}

	var tz *time.Location
	if *Local {
		tz = time.Local
	} else {
		tz = time.UTC
	}

	times, lines, err := scan(os.Stdin, extract, tz, *Parse, *Quiet, *Only)
	if err != nil {
		fmt.Fprintf(os.Stderr, "snappr: fatal: failed to read stdin: %v\n", err)
		os.Exit(2)
	}

	snapshots := make([]time.Time, 0, len(times))
	snapshotMap := make([]int, 0, len(times))
	for i, t := range times {
		if !t.IsZero() {
			snapshots = append(snapshots, t)
			snapshotMap = append(snapshotMap, i)
		}
	}

	keep, need := snappr.Prune(snapshots, policy)

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
		fmt.Println(lines[i])
	}

	var pruned int
	ndig := digits(len(keep))
	for at, why := range keep {
		if len(why) != 0 {
			ps := make([]string, len(why))
			for i, period := range why {
				ps[i] = period.String()
			}
			fmt.Fprintf(os.Stderr, "snappr: why: keep [%*d/%*d] %s :: %s\n", ndig, at+1, ndig, len(keep), times[at].Format("Mon 2006 Jan _2 15:04:05"), strings.Join(ps, ", "))
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
				fmt.Fprintf(os.Stderr, "snappr: summary: (%s) %s\n", strings.Repeat("*", cdig), period)
			} else if count == 0 {
				fmt.Fprintf(os.Stderr, "snappr: summary: (%*d) %s\n", cdig, policy.Get(period), period)
			} else {
				fmt.Fprintf(os.Stderr, "snappr: summary: (%*d) %s (missing %d)\n", cdig, policy.Get(period), period, count)
			}
		})
		fmt.Fprintf(os.Stderr, "snappr: summary: pruning %d/%d snapshots\n", pruned, len(keep))
	}
}

func parse(policy ...string) (snappr.Policy, error) {
	var p snappr.Policy

	for _, s := range policy {
		n, u, hasN := strings.Cut(s, "@")
		if !hasN {
			n, u = "-1", n
		}

		u, x, hasX := strings.Cut(u, ":")
		if !hasX {
			x = "1"
		}

		var vu snappr.Unit
		switch u {
		case "last":
			vu = snappr.Last
		case "secondly":
			vu = snappr.Secondly
		case "daily":
			vu = snappr.Daily
		case "monthly":
			vu = snappr.Monthly
		case "yearly":
			vu = snappr.Yearly
		default:
			return p, fmt.Errorf("invalid policy %q: unknown unit %q", s, u)
		}

		vn, err := strconv.ParseInt(n, 10, 64)
		if err != nil {
			return p, fmt.Errorf("invalid policy %q: parse count %q: %w", s, n, err)
		}

		vx, err := strconv.ParseInt(x, 10, 64)
		if vu == snappr.Secondly && err != nil {
			var tmp time.Duration
			tmp, err = time.ParseDuration(x)
			vx = int64(tmp / time.Second)
		}
		if err != nil {
			return p, fmt.Errorf("invalid policy %q: parse interval %q: %w", s, x, err)
		}

		if vu == snappr.Last && vx != 1 {
			return p, fmt.Errorf("invalid policy %q: interval must be 1 for unit last", s)
		}

		if !p.Set(snappr.Period{Unit: vu, Interval: int(vx)}, int(vn)) {
			return p, fmt.Errorf("invalid policy %q: duplicate %s:%d", s, u, vx)
		}
	}

	return p, nil
}

func scan(r io.Reader, extract *regexp.Regexp, tz *time.Location, layout string, quiet, only bool) (times []time.Time, lines []string, err error) {
	sc := bufio.NewScanner(r)
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
				if !quiet {
					fmt.Fprintf(os.Stderr, "snappr: warning: failed to parse unix timestamp %q: %v\n", ts, err)
					bad = true
				}
			} else {
				if only {
					line = m[0]
				}
				ts = m[len(m)-1]
			}
		}

		var t time.Time
		if !bad {
			if layout == "" {
				if n, err := strconv.ParseInt(ts, 10, 64); err != nil {
					if !quiet {
						fmt.Fprintf(os.Stderr, "snappr: warning: failed to parse unix timestamp %q: %v\n", ts, err)
					}
					bad = true
				} else {
					t = time.Unix(n, 0)
				}
			} else {
				if v, err := time.ParseInLocation(layout, ts, tz); err != nil {
					if !quiet {
						fmt.Fprintf(os.Stderr, "snappr: warning: failed to parse timestamp %q using layout %q: %v\n", ts, layout, err)
					}
					bad = true
				} else {
					t = v
				}
			}
		}

		if bad {
			times = append(times, time.Time{})
		} else {
			times = append(times, t)
		}
		lines = append(lines, line)
	}
	return times, lines, sc.Err()
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
