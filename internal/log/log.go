package log

import (
	"log"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func New() *zap.Logger {
	cfg := zap.Config{
		Encoding:         "console",
		Development:      true,
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
		Level:            zap.NewAtomicLevel(),
		EncoderConfig: zapcore.EncoderConfig{
			MessageKey:  "message",
			LevelKey:    "level",
			EncodeLevel: zapcore.CapitalColorLevelEncoder,
		},
	}

	logger, err := cfg.Build()
	if err != nil {
		log.Fatalf("error construct logger: %s", err.Error())
	}

	defer logger.Sync()

	return logger
}
