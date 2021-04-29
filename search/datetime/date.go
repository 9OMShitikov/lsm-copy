package datetime

import (
	"fmt"
	"time"
)

type date struct {
	year  int
	month time.Month
	day   int
}

func (d1 date) String() string {
	return fmt.Sprintf("%d-%02d-%02d", d1.year, d1.month, d1.day)
}

func (d1 date) Before(d2 date) bool {
	return (d1.year < d2.year) || (d1.year == d2.year && d1.month < d2.month) ||
		(d1.year == d2.year && d1.month == d2.month && d1.day < d2.day)
}

func (d1 date) Equal(d2 date) bool {
	return d1.year == d2.year && d1.month == d2.month && d1.day == d2.day
}

func (d1 date) After(d2 date) bool {
	return (d1.year > d2.year) || (d1.year == d2.year && d1.month > d2.month) ||
		(d1.year == d2.year && d1.month == d2.month && d1.day > d2.day)
}

func (d1 date) equalToDate(t time.Time) bool {
	return d1.year == t.Year() && d1.month == t.Month() && d1.day == t.Day()
}

// Add uses fake months 13--16 and fake days up to 32 to simplify binary division.
// Users of Add should exclude fake dates by themselves, when needed
func (d1 date) Add(d2 date) date {
	res := date{d1.year + d2.year, d1.month + d2.month, d1.day + d2.day}
	if res.day > 32 {
		res.day -= 32
		res.month++
	}
	if res.month > 16 {
		res.month -= 16
		res.year++
	}
	return res
}

func (d1 *date) isFake() bool {
	return d1.month != time.Date(d1.year, d1.month, d1.day, 0, 0, 0, 0, time.UTC).Month()
}

// convert fake date 'after the end of a month' / 'after the end of an year' to the beginning of next month / year
func (d1 *date) normalize() {
	if d1.month != time.Date(d1.year, d1.month, d1.day, 0, 0, 0, 0, time.UTC).Month() {
		d1.month++
		d1.day = 1
	}
	if d1.month > time.December {
		d1.year++
		d1.month = 1
	}
}

type dateRange struct {
	from, to date
}

// DateRangeCode can be used to acquire feature ids for a time range
// The date-time representation is following.
// For each date-time field the 0th feature is for year,
// the 1st is whether the month number is between 1 and 8 of between 9 and 12,
// the 2nd is whether the month number is between 1--4 / 9--12 or 5--8,
// ...
// the 4th is whether  the month number is odd;
// 5 -- 9 features are for day in the same manner.
type DateRangeCode struct {
	Year int
	// binary division within a year, 0 for 1st half and 1 for second. See comment on DateFeatureIDFormatter above
	InYearCode []int8
}

func featureOrdersToDate(year int, orders []int) date {
	var month, day, i int

	for i = 0; i < len(orders) && orders[i] < 5; i++ {
		month += 1 << (4 - orders[i])
	}
	for ; i < len(orders); i++ {
		day += 1 << (9 - orders[i])
	}

	return date{year, time.Month(month + 1), day + 1}
}

func (dc DateRangeCode) dateRange() dateRange {
	var fromMonth, i int
	for i = 0; i < len(dc.InYearCode) && i < 4; i++ {
		fromMonth += int(dc.InYearCode[i]) << (3 - i)
	}
	toMonth := fromMonth + 16>>len(dc.InYearCode)

	fromDay := 0
	for i := 4; i < len(dc.InYearCode); i++ {
		fromDay += int(dc.InYearCode[i]) << (8 - i)
	}
	toDay := fromDay
	if len(dc.InYearCode) > 4 {
		toDay = toDay + 32>>(len(dc.InYearCode)-4)
	}

	from := date{dc.Year, time.Month(fromMonth + 1), fromDay + 1}
	to := date{dc.Year, time.Month(toMonth + 1), toDay + 1}
	to.normalize()
	return dateRange{from: from, to: to}
}

// ParseTimeRange returns a sequence of non-overlapping date ranges, and corresponding 'feature DNF' for them.
// Each range can be queried via AND of binary features (or their negatives);
// the whole "from -- to" range can be obtained as OR of individual ranges therefore.
// 'Feature DNF' is a slice of 'conjunctions' to be ORed, where in each 'conjunction'
// the position is the order of the feature to be ANDed,
// and bit sign tells if the operation should be AND (1) or AND NOT (0).
func ParseTimeRange(from, to time.Time) (codes []DateRangeCode) {
	fromDate := date{from.Year(), from.Month(), from.Day()}
	if from.IsZero() {
		fromDate = date{1900, time.January, 1}
	}

	toDate := date{to.Year(), to.Month(), to.Day()}
	if to.IsZero() {
		toDate = date{2156, time.January, 1}
	}

	// let's exclude the larges ranges from the grid, then the smaller and smaller ones
	rangesToExtractFrom := []dateRange{{fromDate, toDate}}

	extractRanges := func(dur date, order, maxOrder int) {
		var newRangesToExtractFrom []dateRange
		for i := range rangesToExtractFrom {
			var curNewRanges []dateRange
			var curExtractedRanges []dateRange
			r := &rangesToExtractFrom[i]
			timeBegin := date{r.from.year, time.January, 1}
			timeEnd := date{r.to.year + 1, time.January, 1}
			for i, t := 0, timeBegin; t.Before(timeEnd); i, t = i+1, t.Add(dur) {
				begin, end := t, t.Add(dur)
				if begin.isFake() {
					continue
				}
				end.normalize()
				if (r.from.Before(begin) || r.from.Equal(begin)) && (r.to.After(end) || r.to.Equal(end)) {
					curExtractedRanges = append(curExtractedRanges, dateRange{from: begin, to: end})
					// code is a binary code for a period within a year here, that is obtained via
					// the number of the period
					x := i << (maxOrder - order)
					var code []int8
					for k := maxOrder - 1; k >= maxOrder-order; k-- {
						code = append(code, int8((x>>k)%2))
					}
					codes = append(codes, DateRangeCode{Year: begin.year, InYearCode: code})
				}
			}
			if len(curExtractedRanges) == 0 {
				curNewRanges = []dateRange{{from: r.from, to: r.to}}
			} else {
				begin := curExtractedRanges[0].from
				end := curExtractedRanges[len(curExtractedRanges)-1].to
				if r.from.Before(begin) && r.to.After(end) {
					curNewRanges = append(curNewRanges, dateRange{from: r.from, to: begin}, dateRange{from: end, to: r.to})
				} else if r.from.Equal(begin) && r.to.After(end) {
					curNewRanges = append(curNewRanges, dateRange{from: end, to: r.to})
				} else if r.from.Before(begin) && r.to.Equal(end) {
					curNewRanges = append(curNewRanges, dateRange{from: r.from, to: begin})
				}
			}
			newRangesToExtractFrom = append(newRangesToExtractFrom, curNewRanges...)
		}
		rangesToExtractFrom = newRangesToExtractFrom
	}

	extractRanges(date{1, 0, 0}, 0, 0)
	for j, months := 1, time.Month(8); months >= 1; j, months = j+1, months/2 {
		extractRanges(date{0, months, 0}, j, 4)
	}
	for j, days := 5, 16; days >= 1; j, days = j+1, days/2 {
		extractRanges(date{0, 0, days}, j, 9)
	}

	return codes
}

// ParseTime returns a binary representation of a time:
// year number and orders for 'active' datetime features
func ParseTime(dateTime time.Time) (year int, featureOrders []int) {
	r := date{dateTime.Year(), dateTime.Month(), dateTime.Day()}
	timeBegin := date{r.year, time.January, 1}
	timeEnd := date{r.year + 1, time.January, 1}

	extractRanges := func(dur date, order int) {
		for i, t := 0, timeBegin; t.Before(timeEnd); i, t = i+1, t.Add(dur) {
			begin, end := t, t.Add(dur)
			if begin.isFake() {
				continue
			}
			end.normalize()
			if (r.After(begin) || r.Equal(begin)) && r.Before(end) {
				if i%2 == 1 {
					featureOrders = append(featureOrders, order)
				}
			}
		}
	}

	for j, months := 1, time.Month(8); months >= 1; j, months = j+1, months/2 {
		extractRanges(date{0, months, 0}, j)
	}
	for j, days := 5, 16; days >= 1; j, days = j+1, days/2 {
		extractRanges(date{0, 0, days}, j)
	}

	return r.year, featureOrders
}
