package main

import (
	"fmt"
	"os"
	"time"
)

type Log struct {
	debugging bool
}

func NewLog() *Log {
	return &Log{debugging: false}
}

func (log *Log) info(f string, a ...interface{}) {
	fmt.Fprintf(os.Stdout, "["+time.Now().Format(time.RFC3339)+"] "+f+"\n", a...)
}

func (log *Log) error(f string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, "["+time.Now().Format(time.RFC3339)+"] "+f+"\n", a...)
}

func (log *Log) debug(f string, a ...interface{}) {
	if log.debugging {
		fmt.Fprintf(os.Stderr, "DEBUG ["+time.Now().Format(time.RFC3339)+"]> "+f+"\n", a...)
	}
}

var log = NewLog()
