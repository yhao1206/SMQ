package protocol

import (
	"io"
)

const (
	ClientInit = iota
	ClientWaitGet
	ClientWaitResponse
)

type StatefulReadWriter interface {
	io.ReadWriter
	GetState() int
	SetState(state int)
	String() string
}

type ClientError struct {
	errStr string
}

func (e ClientError) Error() string {
	return e.errStr
}

var (
	ClientErrInvalid    = ClientError{"E_INVALID"}
	ClientErrBadTopic   = ClientError{"E_BAD_TOPIC"}
	ClientErrBadChannel = ClientError{"E_BAD_CHANNEL"}
	ClientErrBadMessage = ClientError{"E_BAD_MESSAGE"}
)
