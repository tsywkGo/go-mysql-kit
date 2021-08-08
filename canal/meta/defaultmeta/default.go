package defaultmeta

import (
	"fmt"
	"time"

	"github.com/go-mysql-org/go-mysql/schema"
	gocache "github.com/patrickmn/go-cache"
	"github.com/siddontang/go-log/log"
	"github.com/tsywkGo/go-mysql-kit/canal/meta/master"
)

const (
	_defaultExpiration = time.Duration(24*60) * time.Hour
	_cleanupInterval   = time.Duration(24) * time.Hour
)

type Meta struct {
	flavor string
	master *master.Master
	cache  *gocache.Cache
}

func New(opts ...Option) (*Meta, error) {
	meta := new(Meta)
	for _, opt := range opts {
		opt(meta)
	}
	if err := meta.checkBinlogSetting(); err != nil {
		return nil, err
	}
	meta.cache = gocache.New(_defaultExpiration, _cleanupInterval)

	return meta, nil
}

func (m *Meta) Close() error {
	m.cache.Flush()
	return m.master.Close()
}

func (m *Meta) Get(dbName, tbName string) (*schema.Table, error) {
	schemaName := m.encodeSchemaName(dbName, tbName)
	val, ok := m.cache.Get(schemaName)
	if ok {
		return val.(*schema.Table), nil
	}
	tbMeta, err := m.master.GetTableSchema(dbName, tbName)
	if err != nil {
		log.Error("dbName:%s, tbName:%s, get table meta from master error:%s", dbName, tbName, err)
		return nil, err
	}
	m.cache.SetDefault(schemaName, tbName)
	return tbMeta, nil
}

func (m *Meta) Insert(dbName, tbName string, tbMeta *schema.Table) error {
	schemaName := m.encodeSchemaName(dbName, tbName)
	m.cache.SetDefault(schemaName, tbMeta)
	return nil
}

func (m *Meta) Delete(dbName, tbName string) error {
	schemaName := m.encodeSchemaName(dbName, tbName)
	m.cache.Delete(schemaName)
	return nil
}

func (m *Meta) encodeSchemaName(dbName, tbName string) string {
	return fmt.Sprintf("%s.%s", dbName, tbName)
}

func (m *Meta) checkBinlogSetting() error {
	if err := m.master.CheckBinlogRowFormat(); err != nil {
		return err
	}
	if err := m.master.CheckBinlogRowFormat(); err != nil {
		return err
	}
	return nil
}
