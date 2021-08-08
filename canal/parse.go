package canal

import (
	"fmt"

	"github.com/tsywkGo/go-mysql-kit/canal/matcher/common"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/pingcap/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/siddontang/go-log/log"
)

var ErrExcludedTable = errors.New("excluded table meta")

// The action name for sync.
const (
	UpdateAction = "update"
	InsertAction = "insert"
	DeleteAction = "delete"
)

func (c *Canal) parseRotateEvent(logEvent *replication.BinlogEvent) error {
	event := logEvent.Event.(*replication.RotateEvent)
	pos := mysql.Position{
		Name: string(event.NextLogName),
		Pos:  uint32(event.Position),
	}
	c.syncer.UpdatePosition(pos)
	c.syncer.UpdateTimestamp(logEvent.Header.Timestamp)
	return nil
}

func (c *Canal) parseRowsEvent(logEvent *replication.BinlogEvent) error {
	err := c.handleRowsEvent(logEvent)
	if err != nil {
		errType := errors.Cause(err)
		if errType != ErrExcludedTable &&
			errType != schema.ErrTableNotExist &&
			errType != schema.ErrMissingTableMeta {
			return err
		}
	}
	return nil
}

func (c *Canal) parseXIDEvent(logEvent *replication.BinlogEvent) error {
	event := logEvent.Event.(*replication.XIDEvent)
	if event.GSet != nil {
		c.syncer.UpdateGTIDSet(event.GSet)
	}
	return nil
}

func (c *Canal) parseMariadbGTIDEvent(logEvent *replication.BinlogEvent) error {
	event := logEvent.Event.(*replication.MariadbGTIDEvent)
	gSet, err := mysql.ParseMariadbGTIDSet(event.GTID.String())
	if err != nil {
		return err
	}
	if gSet != nil {
		c.syncer.UpdateGTIDSet(gSet)
	}
	return nil
}

func (c *Canal) parseGTIDEvent(logEvent *replication.BinlogEvent) error {
	event := logEvent.Event.(*replication.GTIDEvent)
	u, _ := uuid.FromBytes(event.SID)
	gSet, err := mysql.ParseMysqlGTIDSet(fmt.Sprintf("%s:%d", u.String(), event.GNO))
	if err != nil {
		return err
	}
	if gSet != nil {
		c.syncer.UpdateGTIDSet(gSet)
	}
	return nil
}

func (c *Canal) parseQueryEvent(logEvent *replication.BinlogEvent) error {
	event := logEvent.Event.(*replication.QueryEvent)
	stmts, _, err := c.parser.Parse(string(event.Query), "", "")
	if err != nil {
		return err
	}
	log.Infof("parseQueryEvent query %s", string(event.Query))
	for _, stmt := range stmts {
		nodes := parseStmt(stmt)
		for _, node := range nodes {
			if node.db == "" {
				node.db = string(event.Schema)
			}
			if err = c.updateTableMeta(node); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Canal) updateTableMeta(n *node) error {
	// 更新tableMeta cache
	if n.ttype == CreateDDL {
		ts, err := c.meta.Get(n.db, n.table)
		if err != nil {
			return err
		}
		if ts != nil {
			return nil
		}
	}
	log.Infof("table structure changed, update table meta cache: %s.%s\n", n.db, n.table)
	if err := c.meta.Delete(n.db, n.table); err != nil {
		return err
	}
	return nil
}

func (c *Canal) handleRowsEvent(logEvent *replication.BinlogEvent) error {
	event := logEvent.Event.(*replication.RowsEvent)

	dbName := string(event.Table.Schema)
	tbName := string(event.Table.Table)

	if c.matcher.Match(dbName, tbName) == common.StateTypes.Filter {
		return ErrExcludedTable
	}

	ts, err := c.meta.Get(dbName, tbName)
	if err != nil {
		return err
	}

	var action string
	switch logEvent.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		action = InsertAction
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		action = DeleteAction
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		action = UpdateAction
	default:
		return errors.Errorf("%s not supported now", logEvent.Header.EventType)
	}

	log.Infof("action %s, table %s, rows %v", action, ts.String(), event.Rows)
	return nil
}
