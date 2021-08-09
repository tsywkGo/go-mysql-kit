package defaultsyncer

type snapshot struct {
	GTIDSet   string `json:"gtid_set"`
	Position  string `json:"position"`
	Timestamp uint32 `json:"timestamp"`
}
