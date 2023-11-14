// Package snappr prunes snapshots according to a flexible retention policy.
package snappr

import (
	"cmp"
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

func (u Unit) IsValid() bool {
	return u >= 0 && u < numUnits
}

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

func (u Unit) Compare(other Unit) int {
	return cmp.Compare(u, other)
}

// TimeEquals checks whether a and b are equal when truncated to the provided
// unit.
func (u Unit) TimeEquals(a, b time.Time) bool {
	if !u.IsValid() {
		return false
	}
	a = a.Truncate(-1)
	b = b.Truncate(-1)
	switch u {
	case Last:
		return a.Equal(b)
	case Secondly:
		return a.Unix() == b.Unix()
	case Daily:
		ay, am, ad := a.Date()
		by, bm, bd := b.Date()
		return ay == by && am == bm && ad == bd
	case Monthly:
		ay, am, _ := a.Date()
		by, bm, _ := b.Date()
		return ay == by && am == bm
	case Yearly:
		ay, _, _ := a.Date()
		by, _, _ := b.Date()
		return ay == by

	}
	panic("wtf")
}

// Period is a specific time interval for snapshot retention.
type Period struct {
	Unit     Unit
	Interval int // ignored if Unit is Last (normalized to 1), must be > 0
}

func (p Period) Normalize() (Period, bool) {
	ok := p.Unit.IsValid()
	if p.Unit == Last {
		p.Interval = 1
	} else if p.Interval <= 0 {
		ok = false
	}
	return p, ok
}

func (p Period) String() string {
	p, ok := p.Normalize()
	if !ok {
		return ""
	}
	switch p.Unit {
	case Last:
		return p.Unit.String()
	case Secondly:
		return "every " + (time.Second * time.Duration(p.Interval)).String()
	default:
		k := strings.TrimSuffix(p.Unit.String(), "ly")
		if k == "dai" {
			k = "day"
		}
		if p.Interval == 1 {
			return "every " + k
		}
		return "every " + strconv.Itoa(p.Interval) + " " + k + "s"
	}
}

func (p Period) Compare(other Period) int {
	if x := p.Unit.Compare(other.Unit); x != 0 {
		return x
	}
	return cmp.Compare(p.Interval, other.Interval)
}

// PrevTime gets the previous
func (p Period) PrevTime(t time.Time) time.Time {
	if !p.Unit.IsValid() {
		return time.Time{}
	}
	t = t.Truncate(-1)
	switch p.Unit {
	case Last:
		return t.Add(-1)
	case Secondly:
		return t.Add(-time.Second * time.Duration(p.Interval))
	case Daily:
		return t.AddDate(0, 0, -p.Interval)
	case Monthly:
		return t.AddDate(0, -p.Interval, 0)
	case Yearly:
		return t.AddDate(-p.Interval, 0, 0)
	}
	panic("wtf")
}

// Policy defines a retention policy for snapshots.
//
// All periods are valid and normalized.
type Policy struct {
	count map[Period]int // Period is normalized and valid
}

func (p *Policy) MustSet(unit Unit, interval, count int) {
	if !p.Set(Period{unit, interval}, count) {
		panic("invalid period")
	}
}

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

func (p Policy) Get(period Period) (count int) {
	if p.count != nil {
		if period, ok := period.Normalize(); ok {
			count = p.count[period]
		}
	}
	return
}

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

func (p Policy) Clone() Policy {
	if p.count == nil {
		return Policy{}
	}
	return Policy{maps.Clone(p.count)}
}

// Prune prunes the provided list of snapshots, returning a matching slice of
// periods requring that snapshot, and the remaining number of snapshots
// required to fulfill the original policy.
//
// The timezone doesn't matter and doesn't need to be consistent since snapshots
// are ordered by their UTC time value. The timezone will only affect where
// days/months/years are split for the purpose of determining the calendar
// day/month/year.
//
// Guarantees:
//   - Pruning is reproducible and idempotent.
//   - Adding another snapshot then pruning again will never results in the number of needed snapshots increasing.
//   - There will never be more than one yearly snapshot per calendar year retained due to a yearly rule (and it will be the most recent one in that calendar year).
//   - There will never be more than one monthly snapshot per calendar year/month retained due to a monthly rule (and it will be the most recent one in that calendar year/month).
//   - There will never be more than one daily snapshot per calendar year/month/day retained due to a daily rule (and it will be the most recent one in that calendar year/month/day).
//   - There will never be more than one secondly snapshot per second retained to to a secondly rule (and it will be the most recent one in that second).
//   - The interval between snapshots retained due to a secondly rule will never be smaller than the smallest interval.
func Prune(snapshots []time.Time, policy Policy) (keep [][]Period, need Policy) {
	need = policy.Clone()

	// sort the snapshots descending
	sorted := make([]int, len(snapshots))
	for i := range sorted {
		sorted[i] = i
	}
	slices.SortFunc(sorted, func(a, b int) int {
		return snapshots[a].Compare(snapshots[b])
	})
	slices.Reverse(sorted)

	// figure out which ones to keep
	keep = make([][]Period, len(snapshots))
	lastPeriod := map[Period]time.Time{}
	lastPeriodIdx := map[Period]int{}
	lastUnit := [numUnits]time.Time{}
	for _, idx := range sorted {
		at := snapshots[idx].Truncate(-1) // remove monotonic component

		need.Each(func(period Period, count int) {
			if count == 0 {
				return
			}

			// we don't care about times for the Last unit
			if period.Unit == Last {
				keep[idx] = append(keep[idx], period)
				if count > 0 {
					need.count[period]--
				}
				return
			}

			// check if we need this snapshot for the specified policy
			if last := lastPeriod[period]; !last.IsZero() { // if we already have the first snapshot
				if want := period.PrevTime(last); want.Before(at) { // and on or ahead of schedule
					if !period.Unit.TimeEquals(want, at) { // and not scheduled for one in this period+unit
						return // then skip this snapshot
					}
				}
			}

			// see if can't reuse the existing snapshot for the unit-truncated time (i.e., disregarding the interval)
			if have := lastUnit[period.Unit]; have.IsZero() || !period.Unit.TimeEquals(have, at) { // if another interval already caused a retention for this unit
				lastPeriod[period] = at
				lastPeriodIdx[period] = idx
			}

			// keep the snapshot
			keep[lastPeriodIdx[period]] = append(keep[lastPeriodIdx[period]], period)
			if count > 0 {
				need.count[period]--
			}
		})
	}

	return
}
