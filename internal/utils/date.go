package utils

import (
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
