package main

import (
	"github.com/siddontang/go-log/log"
	"github.com/tsywkGo/go-mysql-kit/canal"
)

func main() {
	cfg, err := canal.NewConfigWithFile("./cmd/config/canal.toml")
	if err != nil {
		log.Fatalf("new canal config error:%s", err)
	}
	c, err := canal.New(cfg)
	if err != nil {
		log.Fatalf("new canal error:%s", err)
	}
	if err := c.Run(); err != nil {
		log.Fatalf("run canal error:%s", err)
	}
}
