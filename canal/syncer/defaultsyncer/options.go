package defaultsyncer

import (
	"math/rand"
	"time"

	"github.com/tsywkGo/go-mysql-kit/canal/syncer/flusher"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

const (
	_defaultHeartbeatPeriod = 200 * time.Millisecond
	_defaultReadTimeout     = 500 * time.Millisecond
	_defaultFlushDuration   = 60 * time.Second
)

type Option func(syncer *Syncer)

func WithSyncerID(id string) Option {
	return func(syncer *Syncer) {
		syncer.id = id
	}
}

func WithBinlogSyncer(cfg *replication.BinlogSyncerConfig) Option {
	return func(syncer *Syncer) {
		if cfg.ServerID == 0 {
			cfg.ServerID = uint32(rand.New(rand.NewSource(time.Now().Unix())).Intn(1000)) + 1001
		}

		syncer.serverID = cfg.ServerID

		if len(cfg.Charset) == 0 {
			cfg.Charset = mysql.DEFAULT_CHARSET
		}

		if len(cfg.Flavor) == 0 {
			cfg.Flavor = mysql.MySQLFlavor
		}

		if cfg.HeartbeatPeriod == 0 {
			cfg.HeartbeatPeriod = _defaultHeartbeatPeriod
		}

		if cfg.ReadTimeout == 0 {
			cfg.ReadTimeout = _defaultReadTimeout
		}

		syncer.binlogSyncer = replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
			ServerID:                cfg.ServerID,
			Flavor:                  cfg.Flavor,
			Host:                    cfg.Host,
			Port:                    cfg.Port,
			User:                    cfg.User,
			Password:                cfg.Password,
			Charset:                 cfg.Charset,
			HeartbeatPeriod:         cfg.HeartbeatPeriod,
			ReadTimeout:             cfg.ReadTimeout,
			UseDecimal:              cfg.UseDecimal,
			ParseTime:               cfg.ParseTime,
			SemiSyncEnabled:         cfg.SemiSyncEnabled,
			MaxReconnectAttempts:    cfg.MaxReconnectAttempts,
			DisableRetrySync:        cfg.DisableRetrySync,
			TimestampStringLocation: cfg.TimestampStringLocation,
			TLSConfig:               cfg.TLSConfig,
		})
	}
}

func WithFlusher(flusher flusher.IFlusher) Option {
	return func(syncer *Syncer) {
		syncer.flusher = flusher
	}
}

func WithFlushDuration(duration time.Duration) Option {
	return func(syncer *Syncer) {
		syncer.flushDuration = duration
	}
}
