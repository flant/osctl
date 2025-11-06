package utils

import (
	"regexp"
	"strings"
	"time"
)

func FormatDate(t time.Time, dateFormat string) string {
	goFormat := ConvertDateFormat(dateFormat)
	return t.Format(goFormat)
}

func ConvertDateFormat(dateFormat string) string {
	goFormat := strings.ReplaceAll(dateFormat, "%Y", "2006")
	goFormat = strings.ReplaceAll(goFormat, "%m", "01")
	goFormat = strings.ReplaceAll(goFormat, "%d", "02")
	return goFormat
}

func ConvertDateFormatToRegex(dateFormat string) string {
	pattern := dateFormat
	pattern = strings.ReplaceAll(pattern, "%Y", `\d{4}`)
	pattern = strings.ReplaceAll(pattern, "%m", `\d{2}`)
	pattern = strings.ReplaceAll(pattern, "%d", `\d{2}`)
	pattern = strings.ReplaceAll(pattern, ".", `\.`)
	pattern = strings.ReplaceAll(pattern, "-", `-`)
	pattern = strings.ReplaceAll(pattern, "_", `_`)

	return pattern
}

func ExtractDateFromIndex(index, dateFormat string) string {
	pattern := ConvertDateFormatToRegex(dateFormat)
	re := regexp.MustCompile(pattern)
	matches := re.FindStringSubmatch(index)

	if len(matches) > 0 {
		return matches[0]
	}

	return ""
}

func IsOlderThanCutoff(name, cutoffDate, dateFormat string) bool {
	extractedDate := ExtractDateFromIndex(name, dateFormat)
	if extractedDate == "" {
		return false
	}

	goFormat := ConvertDateFormat(dateFormat)
	cutoffTime, err := time.Parse(goFormat, cutoffDate)
	if err != nil {
		return false
	}

	itemTime, err := time.Parse(goFormat, extractedDate)
	if err != nil {
		return false
	}

	return itemTime.Before(cutoffTime) || itemTime.Equal(cutoffTime)
}

func GetYesterdayFormatted(dateFormat string) string {
	yesterday := time.Now().AddDate(0, 0, -1)
	return FormatDate(yesterday, dateFormat)
}

func GetDayBeforeYesterdayFormatted(dateFormat string) string {
	dayBeforeYesterday := time.Now().AddDate(0, 0, -2)
	return FormatDate(dayBeforeYesterday, dateFormat)
}

func HasDateInName(name, dateFormat string) bool {
	extractedDate := ExtractDateFromIndex(name, dateFormat)
	return extractedDate != ""
}

func GetLaterCutoffDate(date1, date2, dateFormat string) string {
	if date2 == "" {
		return date1
	}

	goFormat := ConvertDateFormat(dateFormat)
	parsedDate1, err1 := time.Parse(goFormat, date1)
	parsedDate2, err2 := time.Parse(goFormat, date2)

	if err1 != nil || err2 != nil {
		return date1
	}

	if parsedDate2.After(parsedDate1) {
		return date2
	}

	return date1
}

func GroupIndicesByDate(indices []string, dateFormat string) map[string][]string {
	groups := make(map[string][]string)
	for _, indexName := range indices {
		date := ExtractDateFromIndex(indexName, dateFormat)
		if date != "" {
			groups[date] = append(groups[date], indexName)
		}
	}
	return groups
}
