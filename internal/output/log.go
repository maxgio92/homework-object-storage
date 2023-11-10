package output

import (
	"io"

	"github.com/sirupsen/logrus"
)

type Option func(l *logrus.Logger)

func WithLevel(level string) Option {
	return func(logger *logrus.Logger) {
		l, err := logrus.ParseLevel(level)
		if err != nil {
			l = logrus.InfoLevel
		}
		logger.SetLevel(l)
	}
}

func WithOutput(output io.Writer) Option {
	return func(logger *logrus.Logger) {
		logger.SetOutput(output)
	}
}

func NewJSONLogger(opts ...Option) *logrus.Logger {
	logger := logrus.New()
	for _, f := range opts {
		f(logger)
	}

	logger.SetFormatter(&logrus.JSONFormatter{
		DisableTimestamp:  false,
		DisableHTMLEscape: false,
		PrettyPrint:       false,
	})

	return logger
}
