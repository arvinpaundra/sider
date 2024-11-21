package sider

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"

	"github.com/arvinpaundra/sider/internal/log"
	"github.com/arvinpaundra/sider/internal/resp"
	"go.uber.org/zap"
)

type Server struct {
	listener     net.Listener
	lastClientId int64
	logger       *zap.Logger

	cmu     sync.Mutex
	clients map[int64]net.Conn

	smu     sync.Mutex
	storage map[string]string
}

func NewServer(listner net.Listener) *Server {
	return &Server{
		listener:     listner,
		clients:      make(map[int64]net.Conn),
		lastClientId: 0,
		cmu:          sync.Mutex{},
		logger:       log.New(),
		storage:      make(map[string]string),
	}
}

func (s *Server) Start() error {
	s.logger.Info(fmt.Sprintf("listening on %s", s.listener.Addr()))

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return err
		}

		s.cmu.Lock()
		s.lastClientId += 1
		s.clients[s.lastClientId] = conn
		s.cmu.Unlock()

		go s.handleConnection(s.lastClientId, conn)
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

	s.logger.Info("server closed")

	return nil
}

func (s *Server) handleConnection(clientId int64, conn net.Conn) {
	s.logger.With(
		zap.Int64("client_id", clientId),
		zap.String("host", conn.RemoteAddr().String()),
	).Info("client connected")

	defer s.disconnect(clientId, conn)

	for {
		reader := resp.NewReader(conn)
		req, err := reader.Read()
		if err != nil && !errors.Is(err, io.EOF) {
			if errors.Is(err, net.ErrClosed) {
				break
			}

			s.logger.Error(err.Error())
			break
		}

		if len(req.Values) == 0 {
			s.logger.With(
				zap.Int64("client_id", clientId),
			).Error("missing command")

			break
		}

		cmd := req.Values[0].Str

		switch strings.ToUpper(cmd) {
		case "GET":
			err = s.handleGetCommand(conn, req)
		case "SET":
			err = s.handleSetCommand(conn, req)
		default:
			_, err = conn.Write([]byte("+ERR unknown command\r\n"))
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
	).Info("disconnecting client")

	s.cmu.Lock()
	_, ok := s.clients[id]
	if !ok {
		return nil
	}

	delete(s.clients, id)
	s.cmu.Unlock()

	err := conn.Close()
	if err != nil {
		s.logger.With(
			zap.Int64("client_id", id),
		).Error(fmt.Sprintf("failed disconnecting client: %s", err.Error()))

		return err
	}

	return nil
}
