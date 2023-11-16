<h1 align="center">snappr</h1>

<a href="https://pkg.go.dev/github.com/pgaskin/snappr"><img align="right" src="https://pkg.go.dev/badge/github.com/pgaskin/snappr.svg" alt="Go Reference"></a>

**CLI and library for pruning time-based snapshots with a flexible retention policy.**

#### Features

- **Standalone.** \
  Works with any tool or script which can output a list with dates somewhere in it.

- **Approximate snapshot selection.** \
  Snapshots periods are not fixed to specific dates. The first matching snapshot for each period is kept (note that this means you'll usually want to keep at least the last snapshot in addition to whatever other rules you have).

- **Robust retention policies.** \
  Multiple intervals are supported for each period (last, secondly, daily, monthly, yearly). You can have one snapshot every month for 6 months, while also having one every two for 12.

- **Flexible command line interface.** \
  Can extract dates in arbitrary formats from arbitrary parts of a line, preserving the entire line, and ignoring or passing-through unmatched or invalid lines.

- **Zone-aware timestamp handling.** \
  Will work correctly with different timezones, sorting and determining secondly snapshots by the real time, but using the calendar day/month/year from the zoned time.

- **Verbose debugging information.** \
  You can view which intervals caused a specific snapshot to be retained, and whether a retention policy wants more snapshots than it found.

> [!WARNING]
> This tool is still very new. While most functionality has been tested and I am using this as part of my own backup scripts, it may still have rough edges, and the command-line interface and API are subject to change.

#### CLI Example

```bash
# install latest version
$ go install github.com/pgaskin/snappr/cmd/snappr@latest
```

```bash
# install from source
$ go install ./cmd/snappr
```

```bash
# testing a range of dates
$ seq 946684800 $((13+55*60*6)) 1735689600 |
  snappr -sw 1@last 7@daily 6@monthly 4@monthly:6 6@yearly 4@yearly:12 >/dev/null
```

```bash
# simple rsync+btrfs snapshots
$ rsync ... /mnt/bkp/cur/
$ btrfs subvol snap -r /mnt/bkp/cur/ /mnt/bkp/snap.$(date --utc +%Y%m%d-%H%M%S)
$ btrfs subvol list -r /mnt/bkp/ |
  snappr -sw \
    -e 'path snap\.([0-9-]{15})$' -Eqo \
    -p '20060102-150405' \
    1@last 12@secondly:1h 7@daily 4@daily:7 6@monthly 5@yearly yearly:10 |
  cut -d ' ' -f2- |
  xargs --no-run-if-empty btrfs subvolume delete
```

#### CLI Usage

```
usage: /tmp/go-build2822248938/b001/exe/snappr [options] policy...

options:
  -E, --extended-regexp     use full regexp syntax rather than POSIX (see pkg.go.dev/regexp/syntax)
  -e, --extract string      extract the timestamp from each input line using the provided regexp, which must contain up to one capture group
  -h, --help                show this help text
  -v, --invert              output the snapshots to keep instead of the ones to prune
  -o, --only                only print the part of the line matching the regexp
  -p, --parse string        parse the timestamp using the specified Go time format (see pkg.go.dev/time#pkg-constants and the examples below) rather than a unix timestamp
  -Z, --parse-timezone tz   use a specific timezone rather than whatever is set for --timezone if no timezone is parsed from the timestamp itself
  -q, --quiet               do not show warnings about invalid or unmatched input lines
  -s, --summarize           summarize retention policy results to stderr
  -z, --timezone tz         convert all timestamps to this timezone while pruning snapshots (use "local" for the default system timezone) (default UTC)
  -w, --why                 explain why each snapshot is being kept to stderr

time format examples:
  - Mon Jan 02 15:04:05 2006
  - 02 Jan 06 15:04 MST
  - 2006-01-02T15:04:05Z07:00
  - 2006-01-02T15:04:05

policy: N@unit:X
  - keep the last N snapshots every X units
  - omit the N@ to keep an infinite number of snapshots
  - if :X is omitted, it defaults to :1
  - there may only be one N specified for each unit:X pair

unit:
  last       snapshot count (X must be 1)
  secondly   clock seconds (can also use the format #h#m#s, omitting any zeroed units)
  daily      calendar days
  monthly    calendar months
  yearly     calendar years

notes:
  - output lines consist of filtered input lines
  - input is read from stdin, and should consist of unix timestamps (or more if --extract and/or --parse are set)
  - invalid/unmatched input lines are ignored, or passed through if --invert is set (and a warning is printed unless --quiet is set)
  - everything will still work correctly even if timezones are different
  - snapshots are always ordered by their real (i.e., UTC) time
  - if using --parse-in, beware of duplicate timestamps at DST transitions (if the offset isn't included whatever you use as the
    snapshot name, and your timezone has DST, you may end up with two snapshots for different times with the same name.
  - timezones will only affect the exact point at which calendar days/months/years are split
```

#### Library Example

```go
var times []time.Time
// ...

var policy Policy
policy.MustSet(snappr.Yearly, 5, -1)
policy.MustSet(snappr.Yearly, 2, 10)
policy.MustSet(snappr.Yearly, 1, 3)
policy.MustSet(snappr.Monthly, 6, 4)
policy.MustSet(snappr.Monthly, 2, 6)
policy.MustSet(snappr.Daily, 1, 7)
policy.MustSet(snappr.Secondly, int(time.Hour/time.Second), 6)
policy.MustSet(snappr.Last, 1, 3)

keep, need := snappr.Prune(times, policy)
for at, reason := range keep {
    if len(reason) == 0 {
        // delete the snapshot times[at]
    }
}
```
