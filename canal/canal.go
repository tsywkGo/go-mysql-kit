package canal

import (
	"context"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/siddontang/go-log/log"
	"github.com/tsywkGo/go-mysql-kit/canal/matcher"
	"github.com/tsywkGo/go-mysql-kit/canal/matcher/defaultmatcher"
	"github.com/tsywkGo/go-mysql-kit/canal/meta"
	"github.com/tsywkGo/go-mysql-kit/canal/meta/defaultmeta"
	"github.com/tsywkGo/go-mysql-kit/canal/syncer"
	"github.com/tsywkGo/go-mysql-kit/canal/syncer/defaultsyncer"
)

// Canal can sync your MySQL data into everywhere, like Kafka, ElasticSearch, Redis, etc...
// MySQL must open row format for binlog
type Canal struct {
	// 同步配置
	cfg *Config

	// 同步表结构管理
	meta meta.IMeta

	// binlog读取
	syncer syncer.ISyncer

	// binlog解析
	parser *parser.Parser

	// 同步规则
	matcher matcher.IMatcher

	ctx    context.Context
	cancel context.CancelFunc
}

func New(cfg *Config) (*Canal, error) {
	c := new(Canal)

	c.ctx, c.cancel = context.WithCancel(context.Background())

	var err error

	c.meta, err = defaultmeta.New(
		defaultmeta.WithMaster(cfg.MetaConfig.MasterConfig),
		defaultmeta.WithFlavor(cfg.MetaConfig.Flavor),
	)

	// todo: flushClient
	c.syncer, err = defaultsyncer.New(
		defaultsyncer.WithSyncerID(cfg.SyncerConfig.SyncerID),
		defaultsyncer.WithBinlogSyncer(cfg.SyncerConfig.ReplicationConfig),
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	c.parser = parser.New()

	c.matcher, err = defaultmatcher.New(
		defaultmatcher.WithIncludeRegex(cfg.MatcherConfig.IncludeRegex),
		defaultmatcher.WithExcludeRegex(cfg.MatcherConfig.ExcludeRegex),
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return c, nil
}

func (c *Canal) Ctx() context.Context {
	return c.ctx
}

func (c *Canal) GetLatency() uint32 {
	return c.syncer.Latency()
}

func (c *Canal) Close() {
	log.Infof("canal closing...")
	c.cancel()
	_ = c.syncer.Close()
	_ = c.meta.Close()
}

func (c *Canal) Run() error {
	s, err := c.syncer.Start()
	if err != nil {
		log.Errorf("canal syncer start error:%s", err)
		return err
	}

	log.Infof("canal starting...")

	for {
		logEvent, err := s.GetEvent(c.ctx)
		if err != nil {
			return err
		}

		// todo: 更新postion/timestamp
		c.syncer.UpdateLatency(logEvent.Header.Timestamp)

		// The name of the binlog file received in the fake rotate event.
		// It must be preserved until the new position is saved.
		// If log pos equals zero then the received event is a fake rotate event and
		// contains only a name of the next binlog file
		// See https://github.com/mysql/mysql-server/blob/8e797a5d6eb3a87f16498edcb7261a75897babae/sql/rpl_binlog_sender.h#L235
		// and https://github.com/mysql/mysql-server/blob/8cc757da3d87bf4a1f07dcfb2d3c96fed3806870/sql/rpl_binlog_sender.cc#L899
		if logEvent.Header.LogPos == 0 {
			switch event := logEvent.Event.(type) {
			case *replication.RotateEvent:
				log.Infof("canal received fake rotate event, nextLogName:%s", event.NextLogName)
			}
			continue
		}
		if err := c.parseEvent(logEvent); err != nil {
			log.Errorf("canal parse binlog event error:%s", err)
			return err
		}
	}
}

func (c *Canal) parseEvent(logEvent *replication.BinlogEvent) error {
	var err error
	// We only save position with RotateEvent and XIDEvent.
	// For RowsEvent, we can't save the position until meeting XIDEvent
	// which tells the whole transaction is over.
	// If we meet any DDL query, we must save too.
	switch logEvent.Event.(type) {
	case *replication.RotateEvent:
		err = c.parseRotateEvent(logEvent)
	case *replication.RowsEvent:
		err = c.parseRowsEvent(logEvent)
	case *replication.XIDEvent:
		err = c.parseXIDEvent(logEvent)
	case *replication.MariadbGTIDEvent:
		err = c.parseMariadbGTIDEvent(logEvent)
	case *replication.GTIDEvent:
		err = c.parseGTIDEvent(logEvent)
	case *replication.QueryEvent:
		err = c.parseQueryEvent(logEvent)
	}
	return err
}
