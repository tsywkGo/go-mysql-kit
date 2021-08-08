package syncer

type RunModel int

var RunModelVars = struct {
	Local  RunModel
	Remote RunModel
}{
	Local:  0,
	Remote: 1,
}
