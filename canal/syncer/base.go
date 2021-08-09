package syncer

import (
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

type ISyncer interface {
	ID() string
	ServerID() uint32
	GTIDSet() mysql.GTIDSet
	Position() mysql.Position
	Timestamp() uint32
	Latency() uint32

	UpdateGTIDSet(gSet mysql.GTIDSet)
	UpdatePosition(pos mysql.Position)
	UpdateTimestamp(ts uint32)
	UpdateLatency(ts uint32)

	Start() (*replication.BinlogStreamer, error)
	Close() error
}
