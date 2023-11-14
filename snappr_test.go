package snappr

import (
	"fmt"
	"strings"
	"time"
)

// TODO: fuzz it (generating a random policy, and a seed for generating 1000
// random time intervals), checking the guarantees for Prune (and ensuring it
// works adding the times one at a time).

// TODO: some specific unit tests

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
	// last (3), every 1h0m0s (6), every day (7), every 2 months (6), every 6 months (4), every year (3), every 2 years (10), every 5 years (inf)
	// Fri Dec 31 23:55:29 1999 | every 2 years
	// Mon Dec 31 23:34:57 2001 | every 2 years
	// Wed Dec 31 23:53:53 2003 | every 2 years, every 5 years
	// Sat Dec 31 23:53:06 2005 | every 2 years
	// Mon Dec 31 23:52:17 2007 | every 2 years
	// Wed Dec 31 23:41:54 2008 | every 5 years
	// Thu Dec 31 23:51:30 2009 | every 2 years
	// Sat Dec 31 23:40:26 2011 | every year, every 2 years
	// Thu May 31 23:33:05 2012 | every 6 months
	// Wed Oct 31 23:35:45 2012 | every 6 months
	// Mon Dec 31 23:10:18 2012 | every 2 months, every year
	// Thu Jan 31 23:53:21 2013 | every 2 months
	// Sun Mar 31 23:17:06 2013 | every 2 months, every 6 months
	// Fri May 31 23:32:10 2013 | every 2 months
	// Wed Jul 31 23:57:29 2013 | every 2 months
	// Mon Sep  2 23:41:05 2013 | every day
	// Tue Sep  3 23:51:06 2013 | every day
	// Wed Sep  4 23:51:53 2013 | every day
	// Thu Sep  5 23:31:54 2013 | every day
	// Fri Sep  6 23:52:26 2013 | every day
	// Sat Sep  7 23:12:42 2013 | every day
	// Sun Sep  8 16:47:35 2013 | every 1h0m0s
	// Sun Sep  8 18:18:52 2013 | every 1h0m0s
	// Sun Sep  8 19:29:23 2013 | every 1h0m0s
	// Sun Sep  8 20:40:55 2013 | every 1h0m0s
	// Sun Sep  8 22:12:12 2013 | last, every 1h0m0s
	// Sun Sep  8 23:22:43 2013 | last
	// Sun Sep  8 23:33:14 2013 | last, every 1h0m0s, every day, every 2 months, every 6 months, every year, every 2 years, every 5 years
	// last (0), every 1h0m0s (0), every day (0), every 2 months (0), every 6 months (0), every year (0), every 2 years (2), every 5 years (inf)
}

func prand[T ~uint | int | uint8 | int8 | uint16 | int16 | uint32 | int32 | uint64 | int64](max, i T, seed uint64) T {
	notEven := ((seed & 0xAAAAAAAAAAAAAAAA) >> 1) | ((seed & 0x5555555555555555) << 1) | 1
	return (i*T(notEven) + T(seed)) % max
}
