package logging

import (
	"sync"

	log "github.com/sirupsen/logrus"
)

type Logger struct{}

var initOnce sync.Once

func NewLogger() *Logger {
	initOnce.Do(func() {
		log.SetFormatter(&log.JSONFormatter{})
		log.SetLevel(log.InfoLevel)
	})
	return &Logger{}
}

func (l *Logger) Info(message string) {
	log.Info(message)
}

func (l *Logger) Warn(message string) {
	log.Warn(message)
}

func (l *Logger) Error(message string) {
	log.Error(message)
}
