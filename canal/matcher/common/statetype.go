package common

type StateType int8

var StateTypes = struct {
	Default StateType
	Matched StateType
	Filter  StateType
}{
	Default: 0,
	Matched: 1,
	Filter:  2,
}
