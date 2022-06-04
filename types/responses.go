package types

type ServerResponse struct {
	serverId string
	success  bool
	data     interface{}
}
