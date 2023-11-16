package snappr

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestParsePolicy(t *testing.T) {
	for _, tc := range []func(*Policy) string{
		func(p *Policy) string {
			return "sdfsdf"
		},
		func(p *Policy) string {
			return ""
		},
		func(p *Policy) string {
			p.MustSet(Yearly, 1, -1)
			return "yearly"
		},
		func(p *Policy) string {
			p.MustSet(Yearly, 1, -1)
			return "    -2@yearly   "
		},
		func(p *Policy) string {
			return "yearly:0"
		},
		func(p *Policy) string {
			return "last:2"
		},
		func(p *Policy) string {
			return "0@last:1"
		},
		func(p *Policy) string {
			return "daily daily"
		},
		func(p *Policy) string {
			return "secondly:1ns"
		},
		func(p *Policy) string {
			return "secondly:999ms"
		},
		func(p *Policy) string {
			p.MustSet(Secondly, 1, -1)
			return "secondly:1000ms"
		},
		func(p *Policy) string {
			return "a@secondly"
		},
		func(p *Policy) string {
			return "secondly:sdf"
		},
		func(p *Policy) string {
			return "secondly:1h0"
		},
		func(p *Policy) string {
			p.MustSet(Yearly, 5, -1)
			p.MustSet(Yearly, 1, 2)
			p.MustSet(Monthly, 3, 2)
			p.MustSet(Daily, 1, 7)
			p.MustSet(Daily, 7, 4)
			p.MustSet(Secondly, int(2*time.Hour/time.Second), 18)
			p.MustSet(Secondly, 1, 5)
			p.MustSet(Secondly, 60, 5)
			p.MustSet(Secondly, 12345, 2)
			return "  yearly:5\t2@yearly 2@monthly:3 7@daily:1 4@daily:7 18@secondly:2h 5@secondly     5@secondly:60 2@secondly:3h25m45s"
		},
	} {
		t.Run("", func(t *testing.T) {
			var exp Policy
			str := tc(&exp)
			invalid := len(exp.count) == 0 && str != ""

			var act Policy
			err := act.UnmarshalText([]byte(str))
			if err == nil {
				t.Logf("\ninput: %s\npolicy: %s", str, act)
			} else {
				t.Logf("\ninput: %s\nerror: %v", str, err)
			}
			if invalid {
				if err == nil {
					t.Fatalf("parse %q: expected error, got no error (policy: %s)", str, act)
				}
				return
			} else {
				if err != nil {
					t.Fatalf("parse %q: unexpected error (error: %v)", str, err)
				}
			}
			if !maps.Equal(act.count, exp.count) {
				t.Errorf("parse %q: incorrect\nexp %s\nact %s", str, exp, act)
			}

			var act1 Policy
			str1, err := act.MarshalText()
			if err != nil {
				t.Fatalf("marshal policy: unexpected error %v", err)
			} else {
				t.Logf("\ncanonical: %s", string(str1))
			}
			err = act1.UnmarshalText(str1)
			if err != nil {
				t.Fatalf("parse marshaled policy %q: unexpected error %v", string(str1), err)
			}
			if !maps.Equal(act1.count, act.count) {
				t.Errorf("parse %q: parsed marshaled policy is not the same\nexp %s\nact %s", str, act, act1)
			}
			str2, err := act1.MarshalText()
			if err != nil {
				t.Fatalf("marshal policy: unexpected error %v", err)
			}
			if !bytes.Equal(str1, str2) {
				t.Errorf("marshal policy: not reproducible:\nexp %s\nact %s", string(str1), string(str2))
			}
		})
	}
}

// pruneCorrectness checks that guarantees provided by Prune are upheld.
func pruneCorrectness(snapshots []time.Time, policy Policy) error {
	var (
		prevNeed   Policy
		prevSubset = -1
		lastKept   []time.Time
	)
	for i, subset := 0, 0; subset < len(snapshots); i++ {
		allSnapshots := snapshots
		snapshots := snapshots[:subset]

		keep, need := Prune(snapshots, policy)

		/**
		 * Prune "keep" output will be like the input snapshots, but with a
		 * sorted slice of periods preventing a snapshot from being pruned, if
		 * applicable.
		 */
		if a, b := len(keep), len(snapshots); a != b {
			return fmt.Errorf("subset %d: prune output invariants: keep: length %d != input length %d", subset, a, b)
		}
		for _, reason := range keep {
			seen := map[Period]struct{}{}
			for _, period := range reason {
				if _, ok := seen[period]; ok {
					return fmt.Errorf("subset %d: prune output invariants: keep: contains duplicate of period %q", subset, period.String())
				} else {
					seen[period] = struct{}{}
				}
				if _, ok := policy.count[period]; !ok {
					return fmt.Errorf("subset %d: prune output invariants: keep: contains period %q which isn't in the original policy", subset, period.String())
				}
			}
			if !slices.IsSortedFunc(reason, Period.Compare) {
				return fmt.Errorf("subset %d: prune output invariants: keep: reason list is not sorted", subset)
			}
		}

		/**
		 * Prune "need" output will contain the number of additional snapshots
		 * required to fulfill the policy for each period.
		 */
		if a, b := mapKeysSorted(need.count, Period.Compare), mapKeysSorted(policy.count, Period.Compare); !slices.Equal(a, b) {
			return fmt.Errorf("subset %d: prune output invariants: need: keys %q != input policy keys %q", subset, need.String(), policy.String())
		}
		for period, need := range need.count {
			count := policy.count[period]
			if count < 0 {
				if need != -1 {
					return fmt.Errorf("subset %d: prune output invariants: need must be -1 if policy count is infinite, got %d for period %q", subset, need, period.String())
				}
				continue
			}
			if need > count {
				return fmt.Errorf("subset %d: prune output invariants: need: period %q missing %d > wanted %d", subset, period.String(), need, count)
			}
			var have int
			for _, reason := range keep {
				if slices.Contains(reason, period) {
					have++
				}
			}
			if total := need + have; total != count {
				return fmt.Errorf("subset %d: prune output invariants: keep, need: total %d != wanted %d", subset, total, count)
			}
		}

		/**
		 * Pruning is reproducible.
		 */
		rKeep, rNeed := Prune(snapshots, policy)
		if !maps.Equal(rNeed.count, need.count) {
			return fmt.Errorf("subset %d: prune reproducibility: need: does not equal original need", subset)
		}
		if !reflect.DeepEqual(rKeep, keep) {
			return fmt.Errorf("subset %d: prune reproducibility: need: does not equal original keep", subset)
		}

		/**
		 * Adding new snapshots will never result in old ones being removed if
		 * still needed to fulfill the policy (i.e., unless the new snapshots
		 * fit the policy and are newer).
		 */
		if subset != 0 {
			for period, count := range need.count {
				if prevCount := prevNeed.count[period]; prevCount < count {
					return fmt.Errorf("subset %d->%d: prune consistency: previous prune without latest snapshot (%s) wanted %d more snapshots to fulfill the policy, but now it thinks it wants %d, which is more?!?", prevSubset, subset, snapshots[subset-1], prevCount, count)
				}
			}
		}

		/**
		 * Pruning is idempotent.
		 */
		var (
			filteredKeep = make([][]Period, 0, len(snapshots))
			filteredSnap = make([]time.Time, 0, len(snapshots))
		)
		for at, reason := range keep {
			if len(reason) != 0 {
				filteredKeep = append(filteredKeep, reason)
				filteredSnap = append(filteredSnap, snapshots[at])
			}
		}
		iKeep, iNeed := Prune(filteredSnap, policy)
		if !maps.Equal(iNeed.count, need.count) {
			return fmt.Errorf("subset %d: prune idempotentency: need: does not equal original need", subset)
		}
		if !reflect.DeepEqual(iKeep, filteredKeep) {
			return fmt.Errorf("subset %d: prune idempotentency: need: does not equal original keep", subset)
		}

		/**
		 * There will never be more than one snapshot retained per unit
		 * increment due to a period using that unit, even if the intervals are
		 * different (i.e., no more than one yearly snapshot per calendar year
		 * retained due to any yearly rule; same for monthly/calendar month,
		 * daily/calendar day, secondly/second).
		 */
		{
			inc := map[string][]int{}
			for at, reason := range keep {
				for _, period := range reason {
					var key string
					switch period.Unit {
					case Last:
						continue
					case Secondly:
						key = period.Unit.String() + " " + snapshots[at].Truncate(-1).Format("2006-01-02 15:04:05")
					case Daily:
						key = period.Unit.String() + " " + snapshots[at].Truncate(-1).Format("2006-01-02")
					case Monthly:
						key = period.Unit.String() + " " + snapshots[at].Truncate(-1).Format("2006-01")
					case Yearly:
						key = period.Unit.String() + " " + snapshots[at].Truncate(-1).Format("2006")
					default:
						panic("wtf")
					}
					if !slices.Contains(inc[key], at) {
						inc[key] = append(inc[key], at)
					}
				}
			}
			var dup []string
			for what, at := range inc {
				if len(at) > 1 {
					var s []string
					for _, at := range at {
						s = append(s, fmt.Sprintf("%d %s", at, snapshots[at]))
					}
					dup = append(dup, fmt.Sprintf("%s = %s", what, strings.Join(s, ", ")))
				}
			}
			if len(dup) != 0 {
				slices.Sort(dup)
				return fmt.Errorf("subset %d: prune correctness: multiple snapshots retained per unit increment:\n%s", subset, strings.Join(dup, "\n"))
			}
		}

		/**
		 * Incrementally pruning snapshots will result in the same amount of
		 * snapshots as pruning them all at once.
		 */
		if subset != 0 {
			lastKept = append(lastKept, snapshots[prevSubset:]...)
			pKeep, _ := Prune(lastKept, policy)

			var incN, absN int
			lastKept = lastKept[:0]
			for _, reason := range pKeep {
				if len(reason) != 0 {
					incN++
				}
			}
			for at, reason := range keep {
				if len(reason) != 0 {
					lastKept = append(lastKept, snapshots[at])
					absN++
				}
			}

			if incN != absN {
				return fmt.Errorf("subset %d->%d: prune consistency: Prune([:%d])=%d != Prune(Prune([:%d]) + [%d:%d])=%d", prevSubset, subset, subset, absN, prevSubset, prevSubset, subset, incN)
			}
		}

		/**
		 * Add an increasing number of snapshots at a time (if the first 2k and
		 * last 50 work fine wrt the prune consistency checks, it's unlikely
		 * that adding more will fail differently, so there's no need to do it
		 * one at a time -- if a middle check fails, this can always be changed
		 * back to incrementing it one at a time to figure out exactly what
		 * caused the failure).
		 */
		var nextSubset int
		if subset > 2000 && subset+50 < len(allSnapshots) {
			nextSubset = subset + len(allSnapshots)/75
		} else {
			nextSubset = subset + 1
		}
		if nextSubset = min(nextSubset, len(allSnapshots)-1); prevSubset == nextSubset {
			break // we've checked everything
		}
		prevNeed = need
		prevSubset = subset
		subset = nextSubset
	}
	return nil
}

func TestPrune(t *testing.T) {
	for _, tc := range []func() (
		times []time.Time,
		policy Policy,

		// just a hash since there's not much point dumping the entire output
		// here; it's not obvious at a glance if it's correct (it's more obvious
		// for the bad failures), so it's easier just to manually check it every
		// time it changes
		output string,
	){
		func() (times []time.Time, policy Policy, output string) {
			for i := 0; i < 5000*24*2; i++ {
				times = append(times, time.Date(2000, 1, 1, 0, 30*i, prand(30*60, i, 0xABCDEF0123456789), 0, time.UTC))
			}

			policy.MustSet(Yearly, 5, -1)
			policy.MustSet(Yearly, 2, 10)
			policy.MustSet(Yearly, 1, 3)
			policy.MustSet(Monthly, 6, 4)
			policy.MustSet(Monthly, 2, 6)
			policy.MustSet(Daily, 1, 7)
			policy.MustSet(Secondly, int(time.Hour/time.Second), 6)
			policy.MustSet(Last, 1, 3)

			return times, policy, "a48749a9d6e92ebbc09a5fb3b46a304879fdb1aeebe28264c0885cea0048f8d1"
		},
		func() (times []time.Time, policy Policy, output string) {
			t := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
			for i := 0; i < 24*7*90; i++ {
				t = t.Add(time.Hour)
				times = append(times, t)
			}

			policy.MustSet(Last, 1, 1)
			policy.MustSet(Secondly, int(time.Hour/time.Second), 6)
			policy.MustSet(Secondly, int(2*time.Hour/time.Second), 6)
			policy.MustSet(Daily, 1, 7)
			policy.MustSet(Daily, 7, 4)
			policy.MustSet(Monthly, 1, 6)
			policy.MustSet(Monthly, 2, 6)
			policy.MustSet(Yearly, 1, -1)

			return times, policy, "1c5391563aef1a2ae123b3a099c00b7635752e64f7a259e4ca4cf32e600e7395"
		},
		// TODO: more cases
	} {
		t.Run("", func(t *testing.T) {
			times, policy, output := tc()

			if times1, policy1, output1 := tc(); !reflect.DeepEqual(times, times1) || !reflect.DeepEqual(policy, policy1) || output != output1 {
				panic("inconsistent test case generator")
			}

			t.Run("Output", func(t *testing.T) {
				keep, need := Prune(times, policy)

				var b bytes.Buffer
				for at, reason := range keep {
					at := times[at]
					if len(reason) != 0 {
						b.WriteString(at.Format(time.ANSIC))
						b.WriteString(" | ")
						for i, r := range reason {
							if i != 0 {
								b.WriteString(", ")
							}
							b.WriteString(r.String())
						}
						b.WriteString("\n")
					}
				}
				b.WriteString(need.String())
				b.WriteString("\n")

				t.Log("\n" + b.String())

				hash := sha256.Sum256(b.Bytes())
				actual := hex.EncodeToString(hash[:])
				if actual != output {
					t.Errorf("incorrect output hash %q", actual)
				}
			})

			t.Run("Correctness", func(t *testing.T) {
				if err := pruneCorrectness(times, policy); err != nil {
					t.Error(err.Error())
				}
			})
		})
	}
}

// TODO: fuzz it (generating a random policy, and a seed for generating 1000
// random time intervals), checking the guarantees for Prune (and ensuring it
// works adding the times one at a time).

func ExamplePrune() {
	var times []time.Time
	for i := 0; i < 5000*24*2; i++ {
		times = append(times, time.Date(2000, 1, 1, 0, 30*i, prand(30*60, i, 0xABCDEF0123456789), 0, time.UTC))
	}

	var policy Policy
	policy.MustSet(Yearly, 5, -1)
	policy.MustSet(Yearly, 2, 10)
	policy.MustSet(Yearly, 1, 3)
	policy.MustSet(Monthly, 6, 4)
	policy.MustSet(Monthly, 2, 6)
	policy.MustSet(Daily, 1, 7)
	policy.MustSet(Secondly, int(time.Hour/time.Second), 6)
	policy.MustSet(Last, 1, 3)
	fmt.Println(policy)

	keep, need := Prune(times, policy)
	for at, reason := range keep {
		at := times[at]
		if len(reason) != 0 {
			var b strings.Builder
			for i, r := range reason {
				if i != 0 {
					b.WriteString(", ")
				}
				b.WriteString(r.String())
			}
			fmt.Println(at.Format(time.ANSIC), "|", b.String())
		}
	}
	fmt.Println(need)

	// Output:
	// last (3), 1h time (6), 1 day (7), 2 month (6), 6 month (4), 1 year (3), 2 year (10), 5 year (inf)
	// Fri Dec 31 23:55:29 1999 | 2 year, 5 year
	// Sat Jan  1 00:36:00 2000 | 2 year, 5 year
	// Tue Jan  1 00:45:28 2002 | 2 year
	// Thu Jan  1 00:04:24 2004 | 2 year
	// Sat Jan  1 00:04:16 2005 | 5 year
	// Sun Jan  1 00:43:52 2006 | 2 year
	// Tue Jan  1 00:02:48 2008 | 2 year
	// Fri Jan  1 00:42:16 2010 | 2 year, 5 year
	// Sat Jan  1 00:11:21 2011 | 1 year
	// Thu Dec  1 00:18:09 2011 | 6 month
	// Sun Jan  1 00:01:12 2012 | 1 year, 2 year
	// Fri Jun  1 00:43:36 2012 | 6 month
	// Mon Oct  1 00:13:28 2012 | 2 month
	// Sat Dec  1 00:38:47 2012 | 2 month, 6 month
	// Tue Jan  1 00:01:04 2013 | 1 year
	// Fri Feb  1 00:33:52 2013 | 2 month
	// Mon Apr  1 00:27:37 2013 | 2 month
	// Sat Jun  1 00:12:41 2013 | 2 month, 6 month
	// Thu Aug  1 00:38:00 2013 | 2 month
	// Mon Sep  2 00:01:04 2013 | 1 day
	// Tue Sep  3 00:31:51 2013 | 1 day
	// Wed Sep  4 00:01:37 2013 | 1 day
	// Thu Sep  5 00:32:24 2013 | 1 day
	// Fri Sep  6 00:12:25 2013 | 1 day
	// Sat Sep  7 00:43:12 2013 | 1 day
	// Sun Sep  8 00:03:28 2013 | 1 day
	// Sun Sep  8 18:18:52 2013 | 1h time
	// Sun Sep  8 19:09:38 2013 | 1h time
	// Sun Sep  8 20:20:09 2013 | 1h time
	// Sun Sep  8 21:51:26 2013 | 1h time
	// Sun Sep  8 22:01:57 2013 | 1h time
	// Sun Sep  8 22:12:12 2013 | last
	// Sun Sep  8 23:22:43 2013 | last, 1h time
	// Sun Sep  8 23:33:14 2013 | last
	// last (0), 1h time (0), 1 day (0), 2 month (0), 6 month (0), 1 year (0), 2 year (2), 5 year (inf)
}

func prand[T ~uint | int | uint8 | int8 | uint16 | int16 | uint32 | int32 |
	uint64 | int64](max, i T, seed uint64) T {
	notEven := ((seed & 0xAAAAAAAAAAAAAAAA) >> 1) | ((seed & 0x5555555555555555) << 1) | 1
	return (i*T(notEven) + T(seed)) % max
}

func mapKeysSorted[M ~map[K]V, K comparable, V any](m M, compare func(K, K) int) []K {
	if m == nil {
		return nil
	}
	ks := make([]K, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	slices.SortFunc(ks, compare)
	return ks
}
