// Package snappr prunes snapshots according to a flexible retention policy.
package snappr

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Unit represents a precision and unit of measurement.
type Unit int

const (
	Last     Unit = iota // snapshot count
	Secondly             // wallclock seconds
	Daily                // calendar days
	Monthly              // calendar months
	Yearly               // calendar years
	numUnits
)

// IsValid checks if the unit is known.
func (u Unit) IsValid() bool {
	return u >= 0 && u < numUnits
}

// String returns the name of the unit, which is identical to the constant name,
// but in lowercase.
func (u Unit) String() string {
	if !u.IsValid() {
		return ""
	}
	switch u {
	case Last:
		return "last"
	case Secondly:
		return "secondly"
	case Daily:
		return "daily"
	case Monthly:
		return "monthly"
	case Yearly:
		return "yearly"
	}
	panic("wtf")
}

// Compare strictly compares two units.
func (u Unit) Compare(other Unit) int {
	return cmp.Compare(u, other)
}

// Period is a specific time interval for snapshot retention.
type Period struct {
	Unit     Unit
	Interval int // ignored if Unit is Last (normalized to 1), must be > 0
}

// Normalize validates and canonicalizes a period.
func (p Period) Normalize() (Period, bool) {
	ok := p.Unit.IsValid()
	if p.Unit == Last {
		p.Interval = 1
	} else if p.Interval <= 0 {
		ok = false
	}
	return p, ok
}

// String formats the period in a human-readable form. The exact output is
// subject to change.
func (p Period) String() string {
	p, ok := p.Normalize()
	if !ok {
		return ""
	}
	switch p.Unit {
	case Last:
		return p.Unit.String()
	case Secondly:
		s := (time.Second * time.Duration(p.Interval)).String()
		if v, ok := strings.CutSuffix(s, "m0s"); ok {
			s = v + "m"
		}
		if v, ok := strings.CutSuffix(s, "h0m"); ok {
			s = v + "h"
		}
		return s + " time"
	default:
		k := strings.TrimSuffix(p.Unit.String(), "ly")
		if k == "dai" {
			k = "day"
		}
		return strconv.Itoa(p.Interval) + " " + k
	}
}

// Compare strictly compares the provided periods.
func (p Period) Compare(other Period) int {
	if x := p.Unit.Compare(other.Unit); x != 0 {
		return x
	}
	return cmp.Compare(p.Interval, other.Interval)
}

// Policy defines a retention policy for snapshots.
//
// All periods are valid and normalized.
type Policy struct {
	count map[Period]int // Period is normalized and valid
}

// MustSet is like Set, but panics if the period is invalid or has already been
// used.
func (p *Policy) MustSet(unit Unit, interval, count int) {
	if p.Get(Period{unit, interval}) != 0 {
		panic("duplicate period")
	}
	if !p.Set(Period{unit, interval}, count) {
		panic("invalid period")
	}
}

// Set sets the count for a period if it is valid, replacing any existing count.
// A count of zero removes the period.
func (p *Policy) Set(period Period, count int) (ok bool) {
	if count < 0 {
		count = -1
	}
	period, ok = period.Normalize()
	if ok {
		if p.count == nil {
			p.count = map[Period]int{}
		}
		if count == 0 {
			delete(p.count, period)
		} else {
			p.count[period] = count
		}
	}
	return
}

// Get gets the count for a period if it is set.
func (p Policy) Get(period Period) (count int) {
	if p.count != nil {
		if period, ok := period.Normalize(); ok {
			count = p.count[period]
		}
	}
	return
}

// Each loops over all periods in order.
func (p Policy) Each(fn func(period Period, count int)) {
	if p.count != nil {
		periods := make([]Period, 0, len(p.count))
		for period := range p.count {
			periods = append(periods, period)
		}
		slices.SortFunc(periods, Period.Compare)

		for _, period := range periods {
			fn(period, p.count[period])
		}
	}
}

// String formats the policy in a human-readable form. The exact output is
// subject to change.
func (p Policy) String() string {
	var b []byte
	p.Each(func(period Period, count int) {
		if b != nil {
			b = append(b, ',', ' ')
		}
		b = append(b, period.String()...)
		b = append(b, ' ', '(')
		if count < 0 {
			b = append(b, "inf"...)
		} else {
			b = strconv.AppendInt(b, int64(count), 10)
		}
		b = append(b, ')')
	})
	return string(b)
}

// Clone returns a copy of the policy.
func (p Policy) Clone() Policy {
	if p.count == nil {
		return Policy{}
	}
	return Policy{maps.Clone(p.count)}
}

// ParsePolicy parses a policy from the provided rules.
//
// Each rule is in the form N@unit:X, where N is the snapshot count, unit is a
// unit name, and X is the interval. If N is negative, an infinite number of
// snapshots is retained. N must not be zero. X must be greater than zero. If N@
// is omitted, it defaults to -1. If :X is omitted, it defaults to 1. For the
// "last" unit, X must be 1. For the "secondly" unit, X can also be a duration
// in the format used by [time.ParseDuration]. Each rule must be unique by the
// unit:X.
func ParsePolicy(rule ...string) (Policy, error) {
	var p Policy

	for _, s := range rule {
		n, u, hasN := strings.Cut(s, "@")
		if !hasN {
			n, u = "-1", n
		}

		u, x, hasX := strings.Cut(u, ":")
		if !hasX {
			x = "1"
		}

		var vu Unit
		switch strings.ToLower(u) {
		case "last":
			vu = Last
		case "secondly":
			vu = Secondly
		case "daily":
			vu = Daily
		case "monthly":
			vu = Monthly
		case "yearly":
			vu = Yearly
		default:
			return p, fmt.Errorf("rule %q: unknown unit %q", s, u)
		}

		vn, err := strconv.ParseInt(n, 10, 64)
		if err != nil {
			return p, fmt.Errorf("rule %q: parse count %q: %w", s, n, err)
		}
		if vn == 0 {
			return p, fmt.Errorf("rule %q: count must not be zero", s)
		}

		vx, err := strconv.ParseInt(x, 10, 64)
		if vu == Secondly && err != nil {
			var tmp time.Duration
			tmp, err = time.ParseDuration(x)
			vx = int64(tmp / time.Second)
		}
		if err != nil {
			return p, fmt.Errorf("rule %q: parse interval %q: %w", s, x, err)
		}
		if vx < 1 {
			return p, fmt.Errorf("rule %q: interval must be > 0", s)
		}
		if vu == Last && vx != 1 {
			return p, fmt.Errorf("rule %q: interval must be 1 for unit last", s)
		}
		if p.Get(Period{Unit: vu, Interval: int(vx)}) != 0 {
			return p, fmt.Errorf("rule %q: duplicate %s:%d", s, u, vx)
		}
		if !p.Set(Period{Unit: vu, Interval: int(vx)}, int(vn)) {
			return p, fmt.Errorf("rule %q: invalid period %s:%d", s, u, vx)
		}
	}

	return p, nil
}

// UnmarshalText parses the provided text into p, replacing the existing
// policy. It splits the text by whitespace and calls ParsePolicy.
func (p *Policy) UnmarshalText(b []byte) error {
	v, err := ParsePolicy(strings.Fields(string(b))...)
	if err == nil {
		*p = v
	}
	return err
}

// MarshalText encodes the policy into a form usable by UnmarshalText. The
// output is the canonical form of the rules (i.e., all equivalent policies will
// result in the same output).
func (p Policy) MarshalText() ([]byte, error) {
	var b []byte
	p.Each(func(period Period, count int) {
		if b != nil {
			b = append(b, ' ')
		}
		if count > 0 {
			b = strconv.AppendInt(b, int64(count), 10)
			b = append(b, '@')
		}
		b = append(b, period.Unit.String()...)
		if period.Interval != 1 {
			b = append(b, ':')
			if period.Unit == Secondly && period.Interval >= 60 {
				s := (time.Second * time.Duration(period.Interval)).String()
				if v, ok := strings.CutSuffix(s, "m0s"); ok {
					s = v + "m"
				}
				if v, ok := strings.CutSuffix(s, "h0m"); ok {
					s = v + "h"
				}
				b = append(b, s...)
			} else {
				b = strconv.AppendInt(b, int64(period.Interval), 10)
			}
		}
	})
	return b, nil
}

// Prune prunes the provided list of snapshots, returning a matching slice of
// periods requiring that snapshot, and the remaining number of snapshots
// required to fulfill the original policy.
//
// All snapshots are placed in the provided timezone, and the monotonic time
// component is removed. The timezone affects the exact point at which calendar
// days/months/years are split. Beware of duplicate timestamps at DST
// transitions (if the offset isn't included whatever you use as the snapshot
// name, and your timezone has DST, you may end up with two snapshots for
// different times with the same name).
//
// See pruneCorrectness in snappr_test.go for some additional notes about
// guarantees provided by Prune.
func Prune(snapshots []time.Time, policy Policy, loc *time.Location) (keep [][]Period, need Policy) {
	need = policy.Clone()
	keep = make([][]Period, len(snapshots))

	if len(snapshots) == 0 {
		return
	}

	// sort the snapshots descending
	sorted := make([]int, len(snapshots))
	for i := range sorted {
		sorted[i] = i
	}
	slices.SortFunc(sorted, func(a, b int) int {
		return snapshots[a].Compare(snapshots[b])
	})

	policy.Each(func(period Period, count int) {
		var (
			match = make([]bool, len(snapshots))
			last  int64 // period index
			prev  bool
		)
		// start from the beginning, marking the first one in each period
		for i := range snapshots {
			var current int64
			switch t := snapshots[sorted[i]].In(loc).Truncate(-1); period.Unit {
			case Last:
				match[i] = true
				continue
			case Secondly:
				current = t.Unix()
			case Daily:
				n, x := t.Year(), 0

				x = n / 400
				current += int64(x * (365*400 + 97)) // days per 400 years
				n -= x * 400

				x = n / 100
				current += int64(x * (365*100 + 24)) // days per 100 years
				n -= x * 100

				x = n / 4
				current += int64(x * (365*4 + 1)) // days per 4 years
				n -= x * 4

				current += int64(x) + int64(t.YearDay())
			case Monthly:
				year, month, _ := t.Date()
				current = (int64(year)*12 + int64(month))
			case Yearly:
				current = int64(t.Year())
			default:
				panic("wtf")
			}
			current /= int64(period.Interval)

			if !prev || current != last {
				match[i] = true
				last = current
				prev = true
			}
		}
		// preserve from the end and stay within the count
		for i := range match {
			i = len(match) - 1 - i
			if count == 0 {
				break
			}
			if !match[i] {
				continue
			}
			if count > 0 {
				count--
			}
			keep[sorted[i]] = append(keep[sorted[i]], period)
		}
		need.count[period] = count
	})
	return
}
