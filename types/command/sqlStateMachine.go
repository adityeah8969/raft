package command

type SqlStateMachineCmd struct {
	Action string `json:"action"`
	Table  string `json:"table"`
	Data   string `json:"data"`
	Field  string `json:"field"`
}
