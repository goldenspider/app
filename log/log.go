package log

import (
	"fmt"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type LoggerConfig struct {
	Development bool
	Level       string
}

type Logger struct {
	*zap.SugaredLogger
	cfg zap.Config
}

func NewLogger(cfg *LoggerConfig, opts ...zap.Option) (*Logger, error) {
	logger := &Logger{}
	logger.cfg = zap.NewProductionConfig()
	logger.cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	logger.cfg.Development = cfg.Development

	if 0 != len(cfg.Level) {
		if err := logger.cfg.Level.UnmarshalText([]byte(cfg.Level)); err != nil {
			return nil, fmt.Errorf("failed to parse logger level. error = %v, level = %s", err, cfg.Level)
		}
	}

	l, e := logger.cfg.Build()
	if e != nil {
		return nil, fmt.Errorf("failed to build logger. error = %v, cfg = %#v", e, logger.cfg)
	}

	if 0 != len(opts) {
		l = l.WithOptions(opts...)
	}

	logger.SugaredLogger = l.Sugar()
	return logger, nil
}

func (l *Logger) L() *zap.Logger {
	if nil == l.SugaredLogger {
		return zap.L()
	}
	return l.SugaredLogger.Desugar()
}

func (l *Logger) IsLogEanbled(level zapcore.Level) bool {
	return l.cfg.Level.Enabled(level)
}

func (l *Logger) SetLevel(s string) error {
	old := l.cfg.Level.String()
	if old != s {
		n := zap.NewAtomicLevel()
		if err := n.UnmarshalText([]byte(s)); err != nil {
			return fmt.Errorf("failed to change logger level. wrong level = %s", s)
		}

		if err := l.cfg.Level.UnmarshalText([]byte(s)); err != nil {
			return fmt.Errorf("failed to change logger level. wrong level = %s", s)
		}
		zap.S().Warnf("changed logger level from %s to %s.", old, s)
	}
	return nil
}
