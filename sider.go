package sider

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/arvinpaundra/sider/internal/resp"
	"go.uber.org/zap"
)

const nShard = 1000

type Server struct {
	listener     net.Listener
	lastClientId int64
	isStarted    atomic.Bool
	logger       *zap.Logger

	cmu     sync.Mutex
	clients map[int64]net.Conn

	smu     [nShard]sync.RWMutex
	storage [nShard]map[string]string
}

func NewServer(listner net.Listener) *Server {
	server := &Server{
		listener:     listner,
		clients:      make(map[int64]net.Conn),
		lastClientId: 0,
		isStarted:    atomic.Bool{},
		cmu:          sync.Mutex{},
		logger:       zap.NewNop(),
		smu:          [nShard]sync.RWMutex{},
		storage:      [nShard]map[string]string{},
	}

	for i := range nShard {
		server.storage[i] = make(map[string]string)
	}

	return server
}

func (s *Server) Start() error {
	s.logger.Debug(fmt.Sprintf("listening on %s", s.listener.Addr()))

	if !s.isStarted.CompareAndSwap(false, true) {
		return fmt.Errorf("server is already started")
	}

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return err
		}

		s.cmu.Lock()
		s.lastClientId += 1
		clientId := s.lastClientId
		s.clients[clientId] = conn
		s.cmu.Unlock()

		go s.handleConnection(clientId, conn)
	}
}

func (s *Server) Stop() error {
	for id, conn := range s.clients {
		s.disconnect(id, conn)
	}

	err := s.listener.Close()
	if err != nil {
		return err
	}

	s.logger.Debug("server closed")

	return nil
}

func (s *Server) handleConnection(clientId int64, conn net.Conn) {
	s.logger.With(
		zap.Int64("client_id", clientId),
		zap.String("host", conn.RemoteAddr().String()),
	).Debug("client connected")

	defer s.disconnect(clientId, conn)

	reader := resp.NewReader(conn)
	writer := newBufferWriter(conn)
	go func() {
		if err := writer.Start(); err != nil {
			s.logger.Error(err.Error())
		}
	}()

	for {
		req, err := reader.Read()
		if err != nil && !errors.Is(err, io.EOF) {
			if errors.Is(err, net.ErrClosed) {
				break
			}

			s.logger.Error(err.Error())
			break
		}

		if len(req.Values) == 0 {
			break
		}

		cmd := req.Values[0].Str

		switch strings.ToUpper(cmd) {
		case "GET":
			err = s.handleGetCommand(writer, req)
		case "SET":
			err = s.handleSetCommand(writer, req)
		case "DEL":
			err = s.handleDelete(writer, req)
		default:
			_, err = writer.Write([]byte("-ERR unknown command\r\n"))
		}

		if err != nil {
			s.logger.Error(err.Error())
			break
		}
	}
}

func (s *Server) disconnect(id int64, conn net.Conn) error {
	s.logger.With(
		zap.Int64("client_id", id),
	).Debug("disconnecting client")

	s.cmu.Lock()
	defer s.cmu.Unlock()

	_, ok := s.clients[id]
	if !ok {
		return nil
	}

	delete(s.clients, id)

	err := conn.Close()
	if err != nil {
		s.logger.With(
			zap.Int64("client_id", id),
		).Error(fmt.Sprintf("failed disconnecting client: %s", err.Error()))

		return err
	}

	return nil
}

func calculateShard(s string) int {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)

	hash := uint64(offset64)
	for i := 0; i < len(s); i++ {
		hash ^= uint64(s[i])
		hash *= prime64
	}

	return int(hash % uint64(nShard))
}
