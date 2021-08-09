package canal

import (
	"io/ioutil"

	"github.com/BurntSushi/toml"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/errors"
	"github.com/tsywkGo/go-mysql-kit/canal/meta/master"
)

type Config struct {
	SyncerConfig struct {
		SyncerID            string                          `toml:"syncer_id"`
		ReplicationConfig   *replication.BinlogSyncerConfig `toml:"replication_config"`
		FlushDir            string                          `toml:"flush_dir"`
		FlushDurationSecond int64                           `toml:"flush_duration_second"`
	} `toml:"syncer_config"`

	MetaConfig struct {
		MasterConfig *master.Config `json:"master_config"`
		Flavor       string         `toml:"flavor"`
	} `toml:"meta_config"`

	MatcherConfig struct {
		IncludeRegex string `toml:"include_regex"`
		ExcludeRegex string `toml:"exclude_regex"`
	} `toml:"matcher_config"`
}

func NewConfigWithFile(name string) (*Config, error) {
	data, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return NewConfig(string(data))
}

func NewConfig(data string) (*Config, error) {
	var c Config

	_, err := toml.Decode(data, &c)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &c, nil
}
