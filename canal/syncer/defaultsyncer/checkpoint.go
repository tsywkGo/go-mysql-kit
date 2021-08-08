package defaultsyncer

type checkpointData struct {
	GTIDSet   string `json:"gtid_set"`
	Position  string `json:"position"`
	Timestamp uint32 `json:"timestamp"`
}
