package sider

import (
	"fmt"
	"net"

	"github.com/arvinpaundra/sider/internal/resp"
)

func (s *Server) handleGetCommand(conn net.Conn, val resp.RValue) error {
	if len(val.Values) < 2 {
		_, err := conn.Write([]byte("-ERR missing key\r\n"))
		return err
	}

	key := val.Values[1].Str

	s.smu.Lock()
	result, ok := s.storage[key]
	s.smu.Unlock()

	if ok {
		response := fmt.Sprintf("$%d\r\n%s\r\n", len(result), result)
		_, err := conn.Write([]byte(response))

		return err
	}

	_, err := conn.Write([]byte("_\r\n"))

	return err
}

func (s *Server) handleSetCommand(conn net.Conn, val resp.RValue) error {
	if len(val.Values) < 3 {
		_, err := conn.Write([]byte("-ERR missing key and value\r\n"))
		return err
	}

	key := val.Values[1].Str
	value := val.Values[2].Str

	s.smu.Lock()
	s.storage[key] = value
	s.smu.Unlock()

	_, err := conn.Write([]byte("+OK\r\n"))

	return err
}
