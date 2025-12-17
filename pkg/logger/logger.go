package logger

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"

	"gopkg.in/natefinch/lumberjack.v2"
)

// BaobabLogger is a centralized logging package used by all packages.
// It writes logs to a file as well as prints them to std out.
// It is meant to be a Singleton and accessed by multiple processes, therefore
// is built to be concurrency safe. New logs are added to a queue which processes
// them individually to maintain total order.
// Log files have a max size after which the are compressed and a new file is
// initialized.
// Various log mode determine what logs will be printed to std out.
// DEBUG mode will print all types of logs while PRODUCTIOn mode only prints
// INFO and ERROR level logs

type BaobabLogger struct {
	queue      *LogQueue
	logMode    LogMode
	logger     *slog.Logger
	running    atomic.Bool
	maxLogSize uint64
	mu         sync.Mutex
}

type LogMode int

const (
	DEBUG = iota
	PRODUCTION
)

const (
	LevelDebug slog.Level = -4
	LevelInfo  slog.Level = 0
	LevelWarn  slog.Level = 4
	LevelError slog.Level = 8
)

const (
	LOG_FILE = "baobab.log"
)

var (
	bLogger        *BaobabLogger
	once           sync.Once
	defaultLogMode LogMode = DEBUG
)

// Creates a write requests and adds it to queue
func (l *BaobabLogger) Write(pkg string, fn string, level slog.Level, msg string) error {
	// 1. construct log req
	lItem := LogItem{
		pkg:      pkg,
		fn:       fn,
		logLevel: level,
		msg:      msg,
	}

	lReq := LogReq{
		log: lItem,
	}

	// 2. Send log to queue
	l.queue.addItem(&lReq)

	// 3. Start queue if is not running
	shouldRun := !l.running.Load()

	if shouldRun {
		go l.run()
	}

	return nil
}

// Goes through queue, removes head and writes the log
func (l *BaobabLogger) run() {
	l.running.Store(true)

	var r *LogReq
	for {
		r = l.queue.getOldest()

		if r != nil {
			l.mu.Lock()
			switch r.log.logLevel {
			case LevelInfo:
				l.logger.Info(r.log.msg, "package", r.log.pkg, "function", r.log.fn)
			case LevelDebug:
				l.logger.Debug(r.log.msg, "package", r.log.pkg, "function", r.log.fn)
			case LevelWarn:
				l.logger.Warn(r.log.msg, "package", r.log.pkg, "function", r.log.fn)
			case LevelError:
				l.logger.Error(r.log.msg, "package", r.log.pkg, "function", r.log.fn)
			default:
				l.logger.Error(fmt.Sprintf("Invalid log level. Received: %v", r.log.logLevel))
			}
			l.mu.Unlock()
		}
	}
}

// Returns a single instance of the logger to all processes
// and sets mode as log mode and lSize as the max size of log
// file before rolling
func NewLogger(mode LogMode, lSize uint64) *BaobabLogger {
	if lSize == 0 {
		panic("Invalid log size provided.")
	}

	once.Do(func() {
		var level slog.Level

		if mode == PRODUCTION {
			level = LevelInfo
		} else {
			level = LevelDebug
		}

		lumberjackLogger := &lumberjack.Logger{
			Filename:   LOG_FILE,
			MaxSize:    int(lSize), // MB
			MaxBackups: 3,
			MaxAge:     28, // days
			Compress:   true,
		}

		// create a multiwriter to std out and log file
		wr := io.MultiWriter(os.Stdout, lumberjackLogger)

		handler := slog.NewTextHandler(wr, &slog.HandlerOptions{Level: level})

		logger := slog.New(handler)

		// set log mode
		defaultLogMode = mode

		bLogger = &BaobabLogger{
			queue:      newLogQueue(),
			logMode:    mode,
			logger:     logger,
			maxLogSize: lSize,
		}
	})

	return bLogger
}
