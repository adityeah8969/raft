package logEntry

type SqlStateMcLog struct {
	Term    int
	Index   int
	SqlData Entry
}

type SqlData struct {
	Key string `json:"key"`
	Val string `json:"val"`
}

func (log *SqlStateMcLog) GetTerm() int {
	return log.Term
}

func (log *SqlStateMcLog) GetEntry() Entry {
	return &log.SqlData
}

func (log *SqlStateMcLog) GetIndex() int {
	return log.Index
}
