package canal

import (
	"encoding/json"
	"io/ioutil"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/errors"
	"github.com/siddontang/go-log/log"
	"github.com/tsywkGo/go-mysql-kit/canal/meta/master"
)

type SyncerConfig struct {
	ID                string
	ReplicationConfig *replication.BinlogSyncerConfig
	FlushDir          string
	FlushDuration     time.Duration
}

type MetaConfig struct {
	MasterConfig *master.Config
	Flavor       string
}

type MatcherConfig struct {
	IncludeExpr string `toml:"include_expr"`
	ExcludeExpr string `toml:"exclude_expr"`
}

type Config struct {
	SyncerID            string `toml:"syncer_id"`
	Flavor              string `toml:"flavor"`
	Host                string `toml:"host"`
	Port                uint16 `toml:"port"`
	User                string `toml:"user"`
	Password            string `toml:"password"`
	IncludeExpr         string `toml:"include_expr"`
	ExcludeExpr         string `toml:"exclude_expr"`
	FlushDir            string `toml:"flush_dir"`
	FlushDurationSecond int64  `toml:"flush_duration_second"`
}

func NewConfigWithFile(name string) (*Config, error) {
	data, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, errors.Trace(err)
	}

	c := new(Config)
	if err = toml.Unmarshal(data, c); err != nil {
		return nil, errors.Trace(err)
	}

	log.Infof("canal config:%s", c.String())

	return c, nil
}

func (c *Config) String() string {
	if c == nil {
		return ""
	}
	bytes, _ := json.Marshal(c)
	return string(bytes)
}

func (c *Config) convertSyncerConfig() *SyncerConfig {
	return &SyncerConfig{
		ID: c.SyncerID,
		ReplicationConfig: &replication.BinlogSyncerConfig{
			Flavor:   c.Flavor,
			Host:     c.Host,
			Port:     c.Port,
			User:     c.User,
			Password: c.Password,
		},
		FlushDir:      c.FlushDir,
		FlushDuration: time.Duration(c.FlushDurationSecond) * time.Second,
	}
}

func (c *Config) convertMetaConfig() *MetaConfig {
	return &MetaConfig{
		MasterConfig: &master.Config{
			Host:     c.Host,
			Port:     c.Port,
			User:     c.User,
			Password: c.Password,
		},
		Flavor: c.Flavor,
	}
}

func (c *Config) convertMatcherConfig() *MatcherConfig {
	return &MatcherConfig{
		IncludeExpr: c.IncludeExpr,
		ExcludeExpr: c.ExcludeExpr,
	}
}
