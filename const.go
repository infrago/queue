package queue

import "errors"

const (
	NAME = "QUEUE"
)

var (
	ErrInvalidConnection = errors.New("Invalid queue connection.")
	ErrInvalidMsg        = errors.New("Invalid queue msg.")
	ErrInvalidDeclare    = errors.New("Invalid queue declare.")
	ErrInvalidWeight     = errors.New("Invalid queue connection weight.")
	ErrQueueUnfinished   = errors.New("queue unfinished.")
)
