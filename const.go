package queue

import "errors"

const (
	NAME = "QUEUE"
)

var (
	errInvalidConnection = errors.New("Invalid queue connection.")
	errInvalidMsg        = errors.New("Invalid queue msg.")
	errInvalidWeight     = errors.New("Invalid queue connection weight.")
	errQueueUnfinished   = errors.New("queue unfinished.")
)
