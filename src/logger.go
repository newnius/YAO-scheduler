package main

import (
	_log "github.com/sirupsen/logrus"
	"io"
	"sync"
	"runtime"
	"fmt"
)

type Logger struct {
	LoggerLevel          string          `json:"logger.level"`
	LoggerModuleDisabled map[string]bool `json:"logger.modules_disabled"`
	mu                   sync.Mutex
}

func (logger *Logger) Init() {
	logger.LoggerLevel = "Info"
	logger.LoggerModuleDisabled = map[string]bool{}
}

func (logger *Logger) Debug(args ... interface{}) {
	_log.Debug(args)
}

func (logger *Logger) Info(args ... interface{}) {
	pc, _, _, ok := runtime.Caller(1)
	details := runtime.FuncForPC(pc)
	if ok && details != nil {
		fmt.Printf("called from %s\n", details.Name())
	}
	_log.Info(args)
}

func (logger *Logger) Warn(args ... interface{}) {
	_log.Warn(args)
}

func (logger *Logger) Fatal(args ... interface{}) {
	_log.Fatal(args)
}

func (logger *Logger) Fatalf(format string, args ... interface{}) {
	_log.Fatalf(format, args)
}

func (logger *Logger) SetOutput(f io.Writer) {
	_log.SetOutput(f)
}

func (logger *Logger) SetLoggerLevel(value string) bool {
	switch value {
	case "Debug":
		_log.SetLevel(_log.DebugLevel)
		break
	case "Info":
		_log.SetLevel(_log.InfoLevel)
		break
	case "Warn":
		_log.SetLevel(_log.WarnLevel)
		break
	default:
		return false
	}
	logger.LoggerLevel = value
	_log.Info("logger.level is set to ", value)
	return true
}

func (logger *Logger) LoggerEnableModule(module string) bool {
	logger.mu.Lock()
	defer logger.mu.Unlock()
	if len(module) == 0 {
		return false
	}
	delete(logger.LoggerModuleDisabled, module)
	return true
}

func (logger *Logger) LoggerDisableModule(module string) bool {
	logger.mu.Lock()
	defer logger.mu.Unlock()
	if len(module) == 0 {
		return false
	}
	logger.LoggerModuleDisabled[module] = true
	return true
}

func (logger *Logger) LoggerIsModuleEnabled(module string) bool {
	logger.mu.Lock()
	defer logger.mu.Unlock()
	_, ok := logger.LoggerModuleDisabled[module]
	return !ok
}
