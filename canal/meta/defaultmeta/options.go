package defaultmeta

import (
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/siddontang/go-log/log"
	"github.com/tsywkGo/go-mysql-kit/canal/meta/master"
)

var (
	_supportFlavorSet = map[string]struct{}{
		mysql.MySQLFlavor:   {},
		mysql.MariaDBFlavor: {},
	}
)

type Option func(meta *Meta)

func WithMaster(cfg *master.Config) Option {
	return func(meta *Meta) {
		m, err := master.New(cfg)
		if err != nil {
			log.Fatalf("master config:%s, new master connection error:%s", cfg.String(), err)
		}
		meta.master = m
	}
}

func WithFlavor(flavor string) Option {
	return func(meta *Meta) {
		if _, ok := _supportFlavorSet[flavor]; !ok {
			log.Fatalf("flavor:%s, check error:not support flavor type", flavor)
		}
		meta.flavor = flavor
	}
}
