package main

import (
	"errors"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/arvinpaundra/sider"
)

func main() {
	ln, err := net.Listen("tcp", ":3000")
	if err != nil {
		log.Fatalf("failed init listener: %s", err.Error())
	}

	server := sider.NewServer(ln)

	go func() {
		err := server.Start()
		if err != nil && !errors.Is(err, net.ErrClosed) {
			log.Fatalf("failed start server: %s", err.Error())
		}
	}()

	s := make(chan os.Signal, 1)
	signal.Notify(s, syscall.SIGINT)
	<-s

	err = server.Stop()
	if err != nil {
		log.Fatalf("failed stop server: %s", err.Error())
	}
}
