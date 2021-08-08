package master

import (
	"encoding/json"
	"fmt"
)

const (
	_defaultDBName                 = "information_schema"
	_defaultNetwork                = "tcp"
	_defaultPort            uint16 = 3306
	_defaultConnMaxLifetime        = 100 // 单位:s
	_defaultMaxOpenConns           = 2
	_defaultMaxIdleConns           = 2

	_schemaQueryStatement    = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? and TABLE_NAME = ? LIMIT 1"
	_binlogRowImageStatement = "SHOW GLOBAL VARIABLES LIKE 'binlog_row_image'"
	_binlogFormatStatement   = "SHOW GLOBAL VARIABLES LIKE 'binlog_format'"
)

type Config struct {
	Host            string
	Port            uint16
	User            string
	Password        string
	ConnMaxLifetime int
	MaxOpenConns    int
	MaxIdleConns    int
}

func (c *Config) String() string {
	if c == nil {
		return ""
	}
	bytes, _ := json.Marshal(c)
	return string(bytes)
}

func (c *Config) WithDefault() *Config {
	if c.Port == 0 {
		c.Port = _defaultPort
	}
	if c.ConnMaxLifetime <= 0 {
		c.ConnMaxLifetime = _defaultConnMaxLifetime
	}
	if c.MaxOpenConns == 0 {
		c.MaxOpenConns = _defaultMaxOpenConns
	}
	if c.MaxIdleConns <= 0 {
		c.MaxIdleConns = _defaultMaxIdleConns
	}
	return c
}

func (c *Config) Validate() error {
	if c.MaxOpenConns > 0 && c.MaxIdleConns > c.MaxOpenConns {
		return fmt.Errorf("max_idle_conns must less than max_open_conns")
	}
	if c.MaxOpenConns < 0 {
		return fmt.Errorf("max_open_conns must greater than 1")
	}
	return nil
}

func (c *Config) encodeDSN() string {
	return fmt.Sprintf(
		"%s:%s@%s(%s:%d)/%s",
		c.User, c.Password, _defaultNetwork, c.Host, c.Port, _defaultDBName,
	)
}
