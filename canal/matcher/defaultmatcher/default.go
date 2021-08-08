package defaultmatcher

import (
	"fmt"
	"regexp"
	"sync"

	"github.com/tsywkGo/go-mysql-kit/canal/matcher/common"
)

type Matcher struct {
	// IncludeRegex or ExcludeRegex should contain database name
	// Only a table which matches IncludeRegex and dismatches ExcludeRegex will be processed
	// eg, IncludeRegex : [".*\\.canal"], ExcludeRegex : ["mysql\\..*"]
	//     this will include all database's 'canal' table, except database 'mysql'
	// Default IncludeRegex and ExcludeRegex are empty, this will include all tables
	IncludeRegex []*regexp.Regexp
	ExcludeRegex []*regexp.Regexp

	matchedSetMu sync.RWMutex
	matchedSet   map[string]common.StateType
}

func New(opts ...Option) (*Matcher, error) {
	matcher := new(Matcher)
	for _, opt := range opts {
		opt(matcher)
	}
	matcher.matchedSet = make(map[string]common.StateType, 0)
	return matcher, nil
}

// 如果同时存在匹配与过滤，则过滤优先
// true: 表示匹配上; false: 表示需要被过滤
func (m *Matcher) Match(dbName, tbName string) common.StateType {
	schemaName := m.encodeSchemaName(dbName, tbName)
	state := m.matchState(schemaName)
	if state != common.StateTypes.Default {
		return state
	}

	// 过滤优先
	filterFlag := false
	if m.ExcludeRegex != nil {
		for _, reg := range m.ExcludeRegex {
			if reg.MatchString(schemaName) {
				filterFlag = true
				break
			}
		}
	}

	if filterFlag {
		m.updateMatchedSet(schemaName, common.StateTypes.Filter)
		return common.StateTypes.Filter
	}

	matchFlag := false
	if m.IncludeRegex != nil {
		for _, reg := range m.IncludeRegex {
			if reg.MatchString(schemaName) {
				matchFlag = true
				break
			}
		}
	}

	if matchFlag {
		m.updateMatchedSet(schemaName, common.StateTypes.Matched)
		return common.StateTypes.Matched
	}

	// 未匹配上，则认为需要被过滤
	m.updateMatchedSet(schemaName, common.StateTypes.Filter)
	return common.StateTypes.Filter
}

func (m *Matcher) encodeSchemaName(dbName, tbName string) string {
	return fmt.Sprintf("%s.%s", dbName, tbName)
}

func (m *Matcher) matchState(schemaName string) common.StateType {
	if m.matchedSet == nil {
		return common.StateTypes.Default
	}

	m.matchedSetMu.Lock()
	defer m.matchedSetMu.Unlock()

	state, _ := m.matchedSet[schemaName]
	return state
}

func (m *Matcher) updateMatchedSet(schemaName string, state common.StateType) {
	m.matchedSetMu.Lock()
	defer m.matchedSetMu.Unlock()

	m.matchedSet[schemaName] = state
}
