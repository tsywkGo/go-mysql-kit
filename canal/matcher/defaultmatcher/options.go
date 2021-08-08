package defaultmatcher

import (
	"regexp"
	"strings"

	"github.com/siddontang/go-log/log"
)

type Option func(matcher *Matcher)

func WithIncludeRegex(expr string) Option {
	return func(matcher *Matcher) {
		ss := strings.Split(expr, ",")
		for _, val := range ss {
			reg, err := regexp.Compile(val)
			if err != nil {
				log.Errorf("bad include regexp:%s", val)
				continue
			}
			matcher.IncludeRegex = append(matcher.IncludeRegex, reg)
		}
	}
}

func WithExcludeRegex(expr string) Option {
	return func(matcher *Matcher) {
		ss := strings.Split(expr, ",")
		for _, val := range ss {
			reg, err := regexp.Compile(val)
			if err != nil {
				log.Errorf("bad exclude regexp:%s", val)
				continue
			}
			matcher.ExcludeRegex = append(matcher.ExcludeRegex, reg)
		}
	}
}
