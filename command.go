package sider

import (
	"fmt"
	"io"

	"github.com/arvinpaundra/sider/internal/resp"
)

func (s *Server) handleGetCommand(writer io.Writer, val resp.RValue) error {
	if len(val.Values) < 2 {
		_, err := writer.Write([]byte("-ERR missing key\r\n"))
		return err
	}

	key := val.Values[1].Str

	shard := calculateShard(key)
	s.smu[shard].RLock()
	result, ok := s.storage[shard][key]
	s.smu[shard].RUnlock()

	if ok {
		response := fmt.Sprintf("$%d\r\n%s\r\n", len(result), result)
		_, err := writer.Write([]byte(response))

		return err
	}

	_, err := writer.Write([]byte("_\r\n"))

	return err
}

func (s *Server) handleSetCommand(writer io.Writer, val resp.RValue) error {
	if len(val.Values) < 3 {
		_, err := writer.Write([]byte("-ERR missing key and value\r\n"))
		return err
	}

	key := val.Values[1].Str
	value := val.Values[2].Str

	shard := calculateShard(key)
	s.smu[shard].Lock()
	s.storage[shard][key] = value
	s.smu[shard].Unlock()

	_, err := writer.Write([]byte("+OK\r\n"))

	return err
}
