package util

import (
	"crypto/rand"
	"fmt"
	"io"
	"log"
)

var UuidChan = make(chan []byte, 1000)

func UuidFactory() {
	for {
		UuidChan <- uuid()
	}
}

func uuid() []byte {
	b := make([]byte, 16)
	_, err := io.ReadFull(rand.Reader, b)
	if err != nil {
		log.Fatal(err)
	}
	return b
}

func UuidToStr(b []byte) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}
