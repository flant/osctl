package logging

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type Logger struct {
	level  string
	format string
}

type LogEntry struct {
	Level     string      `json:"level"`
	Message   string      `json:"message"`
	Timestamp string      `json:"timestamp"`
	Fields    interface{} `json:"fields,omitempty"`
}

func NewLogger() *Logger {
	level := os.Getenv("LOG_LEVEL")
	if level == "" {
		level = "info"
	}

	format := os.Getenv("LOG_FORMAT")
	if format == "" {
		format = "json"
	}

	return &Logger{
		level:  level,
		format: format,
	}
}

func (l *Logger) log(level, message string, fields interface{}) {
	if !l.shouldLog(level) {
		return
	}

	entry := LogEntry{
		Level:     level,
		Message:   message,
		Timestamp: time.Now().Format(time.RFC3339),
		Fields:    fields,
	}

	if l.format == "json" {
		jsonLog, _ := json.Marshal(entry)
		fmt.Println(string(jsonLog))
	} else {
		fmt.Printf("[%s] %s: %s", entry.Timestamp, level, message)
		if fields != nil {
			fmt.Printf(" %+v", fields)
		}
		fmt.Println()
	}
}

func (l *Logger) shouldLog(level string) bool {
	levels := map[string]int{
		"debug": 0,
		"info":  1,
		"warn":  2,
		"error": 3,
	}

	currentLevel, exists := levels[l.level]
	if !exists {
		currentLevel = 1
	}

	requestedLevel, exists := levels[level]
	if !exists {
		requestedLevel = 1
	}

	return requestedLevel >= currentLevel
}

func (l *Logger) Info(message string, fields ...interface{}) {
	if len(fields) > 0 {
		l.log("info", message, fields[0])
	} else {
		l.log("info", message, nil)
	}
}

func (l *Logger) Warn(message string, fields ...interface{}) {
	if len(fields) > 0 {
		l.log("warn", message, fields[0])
	} else {
		l.log("warn", message, nil)
	}
}

func (l *Logger) Error(message string, fields ...interface{}) {
	if len(fields) > 0 {
		l.log("error", message, fields[0])
	} else {
		l.log("error", message, nil)
	}
}
