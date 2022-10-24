package logEntry

type Entry interface{}

type LogEntry struct {
	Term  int
	Index int
	Entry interface{}
}
