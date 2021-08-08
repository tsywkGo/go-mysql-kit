package matcher

import "github.com/tsywkGo/go-mysql-kit/canal/matcher/common"

type IMatcher interface {
	Match(dbName, tbName string) common.StateType
}
