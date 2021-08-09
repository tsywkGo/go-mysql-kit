package master

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/pingcap/errors"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go/hack"
)

type Master struct {
	flavor string
	db     *sql.DB
}

func New(cfg *Config) (*Master, error) {
	if err := cfg.WithDefault().Validate(); err != nil {
		log.Printf(" master config:%s, validate error:%s", cfg.String(), err)
		log.Fatalf(" master config:%s, validate error:%s", cfg.String(), err)
	}
	db, err := sql.Open("mysql", cfg.encodeDSN())
	if err != nil {
		log.Printf("dsn:%s, open mysql error:%s", cfg.encodeDSN(), err)
		log.Fatalf("dsn:%s, open mysql error:%s", cfg.encodeDSN(), err)
	}
	// 最大连接周期，超过时间的连接就close
	db.SetConnMaxLifetime(time.Duration(cfg.ConnMaxLifetime) * time.Second)
	// 设置最大连接数
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	// 设置闲置连接数
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	return &Master{db: db}, nil
}

func (m *Master) Close() error {
	return m.db.Close()
}

func (m *Master) GetTableSchema(dbName, tbName string) (*schema.Table, error) {
	ts, err := m.newTable(dbName, tbName)
	if err != nil {
		// check table not exists
		if ok, err1 := m.isTableExist(dbName, tbName); err1 == nil && !ok {
			return nil, schema.ErrTableNotExist
		}
		return nil, err
	}
	return ts, nil
}

func (m *Master) rdsHAHeartBeatSchema(dbName, tbName string) *schema.Table {
	// work around : RDS HAHeartBeat
	// ref : https://github.com/alibaba/canal/blob/Conn/parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/dbsync/LogEventConvert.java#L385
	// issue : https://github.com/alibaba/canal/issues/222
	// This is a common error in RDS that canal can't get HAHealthCheckSchema's meta, so we mock a table meta.
	// If canal just skip and log error, as RDS HA heartbeat interval is very short, so too many HAHeartBeat errors will be logged.
	// mock ha_health_check meta
	tm := &schema.Table{
		Schema:  dbName,
		Name:    tbName,
		Columns: make([]schema.TableColumn, 0, 2),
		Indexes: make([]*schema.Index, 0),
	}
	tm.AddColumn("id", "bigint(20)", "", "")
	tm.AddColumn("type", "char(1)", "", "")
	return tm
}

func (m *Master) isTableExist(dbName string, tbName string) (bool, error) {
	rows, err := m.db.Query(_schemaQueryStatement, dbName, tbName)
	if err != nil {
		return false, err
	}
	flag := false
	if rows.Next() {
		flag = true
	}
	_ = rows.Close()
	return flag, nil
}

func (m *Master) newTable(dbName string, tbName string) (*schema.Table, error) {
	ts := &schema.Table{
		Schema:  dbName,
		Name:    tbName,
		Columns: make([]schema.TableColumn, 0, 16),
		Indexes: make([]*schema.Index, 0, 8),
	}

	if err := m.fetchColumns(dbName, tbName, ts); err != nil {
		return nil, errors.Trace(err)
	}

	if err := m.fetchIndexes(dbName, tbName, ts); err != nil {
		return nil, errors.Trace(err)
	}

	return ts, nil
}

func (m *Master) fetchColumns(dbName, tbName string, tbSchema *schema.Table) error {
	query := fmt.Sprintf("SHOW FULL COLUMNS FROM `%s`.`%s`", dbName, tbName)
	rows, err := m.db.Query(query)
	if err != nil {
		return err
	}
	cols, _ := rows.Columns()
	dest := m.makeScanDest(len(cols))

	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return err
		}
		name, _ := m.convertString(dest[0])
		colType, _ := m.convertString(dest[1])
		collation, _ := m.convertString(dest[2])
		extra, _ := m.convertString(dest[6])
		tbSchema.AddColumn(name, colType, collation, extra)
	}
	_ = rows.Close()
	return nil
}

func (m *Master) fetchIndexes(dbName, tbName string, tbSchema *schema.Table) error {
	query := fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", dbName, tbName)
	rows, err := m.db.Query(query)
	if err != nil {
		return err
	}

	cols, _ := rows.Columns()
	dest := m.makeScanDest(len(cols))

	var lastIndex *schema.Index
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return err
		}
		indexName, _ := m.convertString(dest[2])
		colName, _ := m.convertString(dest[4])
		cardinality, _ := m.convertUint(dest[6])
		if lastIndex == nil || lastIndex.Name != indexName {
			index := tbSchema.AddIndex(indexName)
			index.AddColumn(colName, cardinality)
			// 浅拷贝，获取引用
			lastIndex = index
		} else {
			lastIndex.AddColumn(colName, cardinality)
		}
	}
	_ = rows.Close()
	return m.fetchPrimaryKeyColumns(tbSchema)
}

func (m *Master) fetchPrimaryKeyColumns(tbSchema *schema.Table) error {
	if len(tbSchema.Indexes) == 0 {
		return nil
	}

	pkIndex := tbSchema.Indexes[0]
	if pkIndex.Name != "PRIMARY" {
		return nil
	}

	tbSchema.PKColumns = make([]int, len(pkIndex.Columns))
	for i, pkCol := range pkIndex.Columns {
		tbSchema.PKColumns[i] = tbSchema.FindColumn(pkCol)
	}

	return nil
}

func (m *Master) makeScanDest(size int) []interface{} {
	values := make([][]byte, size)
	dest := make([]interface{}, size)
	for i := range values {
		dest[i] = &values[i]
	}
	return dest
}

func (m *Master) convertString(d interface{}) (string, error) {
	switch v := d.(type) {
	case string:
		return v, nil
	case []byte:
		return hack.String(v), nil
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 64), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case nil:
		return "", nil
	default:
		return "", errors.Errorf("data type is %T", v)
	}
}

func (m *Master) convertUint(d interface{}) (uint64, error) {
	switch v := d.(type) {
	case int:
		return uint64(v), nil
	case int8:
		return uint64(v), nil
	case int16:
		return uint64(v), nil
	case int32:
		return uint64(v), nil
	case int64:
		return uint64(v), nil
	case uint:
		return uint64(v), nil
	case uint8:
		return uint64(v), nil
	case uint16:
		return uint64(v), nil
	case uint32:
		return uint64(v), nil
	case uint64:
		return uint64(v), nil
	case float32:
		return uint64(v), nil
	case float64:
		return uint64(v), nil
	case string:
		return strconv.ParseUint(v, 10, 64)
	case []byte:
		return strconv.ParseUint(string(v), 10, 64)
	case nil:
		return 0, nil
	default:
		return 0, errors.Errorf("data type is %T", v)
	}
}

// CheckBinlogRowImage checks MySQL binlog row image, must be in FULL, MINIMAL, NOBLOB
func (m *Master) CheckBinlogRowImage(flavor string) error {
	if flavor == mysql.MySQLFlavor {
		rows, err := m.db.Query(_binlogRowImageStatement)
		if err != nil {
			return err
		}
		// MySQL has binlog row image from 5.6, so older will return empty
		var name, rowImage string
		for rows.Next() {
			if err := rows.Scan(&name, &rowImage); err != nil {
				return err
			}
		}
		if !strings.EqualFold(rowImage, "FULL") {
			return errors.Errorf("MySQL binlog_row_image must FULL, but %s now", rowImage)
		}
	}
	return nil
}

func (m *Master) CheckBinlogRowFormat() error {
	rows, err := m.db.Query(_binlogFormatStatement)
	if err != nil {
		return err
	}
	var name, rowFormat string
	for rows.Next() {
		if err := rows.Scan(&name, &rowFormat); err != nil {
			return err
		}
	}
	if !strings.EqualFold(rowFormat, "ROW") {
		return errors.Errorf("MySQL binlog_format must ROW, but %s now", rowFormat)
	}
	return nil
}
