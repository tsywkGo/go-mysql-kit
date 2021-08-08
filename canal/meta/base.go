package meta

import "github.com/go-mysql-org/go-mysql/schema"

type IMeta interface {
	Get(dbName, tbName string) (*schema.Table, error)
	Insert(dbName, tbName string, tbMeta *schema.Table) error
	Delete(dbName, tbName string) error
	Close() error
}
