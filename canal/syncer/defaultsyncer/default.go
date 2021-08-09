package defaultsyncer

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/siddontang/go-log/log"
	"github.com/tsywkGo/go-mysql-kit/canal/syncer/flusher"
)

type Syncer struct {
	// 同步ID, 同步信息
	id            string
	flusher       flusher.IFlusher
	flushDuration time.Duration

	serverID     uint32
	binlogSyncer *replication.BinlogSyncer

	sync.RWMutex
	gSet      mysql.GTIDSet
	pos       mysql.Position
	timestamp uint32

	// 同步延迟
	latency uint32
}

func New(opts ...Option) (*Syncer, error) {
	syncer := new(Syncer)
	for _, opt := range opts {
		opt(syncer)
	}
	if err := syncer.initSyncer(); err != nil {
		return nil, err
	}
	return syncer, nil
}

func (s *Syncer) initSyncer() error {
	data, err := s.readSnapshot()
	if err != nil {
		return err
	}
	log.Infof("init syncer data:%+v", data)
	s.timingFlush()
	return nil
}

func (s *Syncer) timingFlush() {
	flushTicker := time.NewTicker(s.flushDuration)
	go func() {
		for {
			select {
			case <-flushTicker.C:
				_ = s.writeSnapshot()
			}
		}
	}()
}

func (s *Syncer) ID() string {
	return s.id
}

func (s *Syncer) ServerID() uint32 {
	return s.serverID
}

func (s *Syncer) GTIDSet() mysql.GTIDSet {
	s.RLock()
	defer s.RUnlock()

	return s.gSet
}

func (s *Syncer) Position() mysql.Position {
	s.RLock()
	defer s.RUnlock()

	return s.pos
}

func (s *Syncer) Timestamp() uint32 {
	s.RLock()
	defer s.RUnlock()

	return s.timestamp
}

func (s *Syncer) Latency() uint32 {
	return atomic.LoadUint32(&s.latency)
}

func (s *Syncer) UpdateGTIDSet(gSet mysql.GTIDSet) {
	log.Debugf("update syncer gtid set %s", gSet.String())

	s.Lock()
	defer s.Unlock()

	s.gSet = gSet
}

func (s *Syncer) UpdatePosition(pos mysql.Position) {
	log.Debugf("update syncer position %s", pos.String())

	s.Lock()
	defer s.Unlock()

	s.pos = pos
}

func (s *Syncer) UpdateTimestamp(ts uint32) {
	log.Debugf("update syncer timestamp %d", ts)

	s.Lock()
	defer s.Unlock()

	s.timestamp = ts
}

func (s *Syncer) UpdateLatency(ts uint32) {
	latency := uint32(time.Now().Unix()) - ts
	if latency < 0 {
		latency = 0
	}
	log.Debugf("update syncer latency %d", latency)
	atomic.StoreUint32(&s.latency, latency)
}

func (s *Syncer) writeSnapshot() error {
	bytes, _ := json.Marshal(&snapshot{GTIDSet: s.GTIDSet().String(), Position: s.Position().String(), Timestamp: s.Timestamp()})
	return s.flusher.Write(s.id, bytes)
}

func (s *Syncer) readSnapshot() (*snapshot, error) {
	bytes, err := s.flusher.Read(s.id)
	if err != nil {
		return nil, err
	}
	if len(bytes) == 0 {
		return &snapshot{}, nil
	}
	data := new(snapshot)
	if err := json.Unmarshal(bytes, &data); err != nil {
		log.Errorf("readSnapshot id:%s, error:%s", s.id, err)
		return nil, err
	}
	return data, nil
}

func (s *Syncer) Start() (*replication.BinlogStreamer, error) {
	gSet := s.GTIDSet()
	if gSet == nil || gSet.String() == "" {
		return s.binlogSyncer.StartSync(s.Position())
	}
	return s.binlogSyncer.StartSyncGTID(gSet)
}

func (s *Syncer) Close() error {
	_ = s.writeSnapshot()
	_ = s.flusher.Close()
	s.binlogSyncer.Close()
	return nil
}
