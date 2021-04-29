package datetime

import (
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseTimeRange(t *testing.T) {
	codes := ParseTimeRange(
		time.Date(2019, time.December, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2025, time.January, 15, 0, 0, 0, 0, time.UTC),
	)
	ranges := make([]dateRange, len(codes))
	for i := range codes {
		ranges[i] = codes[i].dateRange()
	}
	require.Equal(t, ranges, []dateRange{
		{date{2020, 1, 1}, date{2021, 1, 1}},
		{date{2021, 1, 1}, date{2022, 1, 1}},
		{date{2022, 1, 1}, date{2023, 1, 1}},
		{date{2023, 1, 1}, date{2024, 1, 1}},
		{date{2024, 1, 1}, date{2025, 1, 1}},
		{date{2019, 12, 1}, date{2020, 1, 1}},
		{date{2025, 1, 1}, date{2025, 1, 9}},
		{date{2025, 1, 9}, date{2025, 1, 13}},
		{date{2025, 1, 13}, date{2025, 1, 15}},
	})
	require.Equal(t, codes, []DateRangeCode{
		{2020, nil},
		{2021, nil},
		{2022, nil},
		{2023, nil},
		{2024, nil},
		{2019, []int8{1, 0, 1, 1}},             // 2019-12-01 -- 2020-01-01
		{2025, []int8{0, 0, 0, 0, 0, 0}},       // 2025-01-01 -- 2025-01-09
		{2025, []int8{0, 0, 0, 0, 0, 1, 0}},    // 2025-01-09 -- 2025-01-13
		{2025, []int8{0, 0, 0, 0, 0, 1, 1, 0}}, // 2025-01-13 -- 2025-01-15
	})
}

func TestParseTimeRangeRandom(t *testing.T) {
	for i := 0; i < 1000; i++ {
		from, to := randomDate(), randomDate()
		if to.Before(from) {
			from, to = to, from
		}
		codes := ParseTimeRange(from, to)

		ranges := make([]dateRange, len(codes))
		for i := range codes {
			ranges[i] = codes[i].dateRange()
		}
		sort.Slice(ranges, func(i, j int) bool {
			return ranges[i].from.Before(ranges[j].from)
		})

		require.True(t, ranges[0].from.equalToDate(from))
		require.True(t, ranges[len(ranges)-1].to.equalToDate(to))

		for i := 0; i < len(ranges)-1; i++ {
			require.True(t, ranges[i].to.Equal(ranges[i+1].from))
		}
		for i := 1; i < len(ranges); i++ {
			require.True(t, ranges[i].from.Equal(ranges[i-1].to))
		}
	}
}

func TestParseTime(t *testing.T) {
	year, orders := ParseTime(time.Date(2020, time.August, 14, 0, 0, 0, 0, time.UTC))
	require.Equal(t, year, 2020)
	require.Equal(t, orders, []int{2, 3, 4, 6, 7, 9})
	require.Equal(t, date{2020, time.August, 14}, featureOrdersToDate(year, orders))
}

func randomDate() time.Time {
	y, m, d := 1600+rand.Intn(600), time.Month(1+rand.Intn(12)), 1+rand.Intn(32)
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

func TestParseTimeRandom(t *testing.T) {
	for i := 0; i < 1000; i++ {
		inDate := randomDate()
		outDate := featureOrdersToDate(ParseTime(inDate))
		require.True(t, outDate.equalToDate(inDate))
	}
}
