package main

import (
	"mrpc/codec"
	"testing"
	"time"
)

func TestMakeCall(t *testing.T) {

}

func BenchmarkMakeCallGOB(b *testing.B) {
	StartServer()
	time.Sleep(1e9)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MakeCall(codec.GOB)
	}
	b.StopTimer()
	StopServer()
}

func BenchmarkMakeCallMSGP(b *testing.B) {
	StartServer()
	time.Sleep(1e9)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MakeCall(codec.MessagePack)
	}
	b.StopTimer()
	StopServer()
}
