package logEntry

type Entry interface{}

type LogEntry interface {
	GetTerm() int
	GetIndex() int
	GetEntry() Entry
}
