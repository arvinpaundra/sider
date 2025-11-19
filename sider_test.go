package sider_test

import (
	"bytes"
	"errors"
	"io"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arvinpaundra/sider"
)

const sockfilePath string = "/tmp/redisexperiment.sock"

func TestNewServer(t *testing.T) {
	ln, err := net.Listen("unix", sockfilePath)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	defer ln.Close()

	server := sider.NewServer(ln)
	if server == nil {
		t.Error("expected server to be not nil")
		t.FailNow()
	}
}

func TestServer_StartAndStop(t *testing.T) {
	ln, err := net.Listen("unix", sockfilePath)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	server := sider.NewServer(ln)

	go func() {
		_ = server.Start()
	}()

	time.Sleep(100 * time.Millisecond)

	err = server.Stop()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
}

func TestServer_HandleConnection(t *testing.T) {
	ln, err := net.Listen("unix", sockfilePath)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	server := sider.NewServer(ln)

	go func() {
		_ = server.Start()
	}()

	defer server.Stop()

	time.Sleep(100 * time.Millisecond)

	client, err := net.Dial("unix", sockfilePath)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	defer client.Close()

	time.Sleep(100 * time.Millisecond)
}

func TestServer_SetAndGetCommand(t *testing.T) {
	ln, err := net.Listen("unix", sockfilePath)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	defer ln.Close()

	server := sider.NewServer(ln)

	go func() {
		_ = server.Start()
	}()

	defer server.Stop()

	client, err := net.Dial("unix", sockfilePath)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	defer client.Close()

	// SET command
	_, err = client.Write([]byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"))
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	buf := make([]byte, 1024)
	n, err := client.Read(buf)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	if !strings.Contains(string(buf[:n]), "+OK") {
		t.Error("expected +OK response")
		t.FailNow()
	}

	// GET command
	_, err = client.Write([]byte("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"))
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	n, err = client.Read(buf)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	if !strings.Contains(string(buf[:n]), "value") {
		t.Error("expected value response")
		t.FailNow()
	}
}

// goos: darwin
// goarch: arm64
// pkg: github.com/arvinpaundra/sider
// cpu: Apple M2
// BenchmarkServer_SetCommand
// BenchmarkServer_SetCommand-8    13105050              1377 ns/op            726098 ops/sec           374 B/op         16 allocs/op

// goos: darwin
// goarch: arm64
// pkg: github.com/arvinpaundra/sider
// cpu: Apple M2
// BenchmarkServer_SetCommand
// BenchmarkServer_SetCommand-8    10000000               292.8 ns/op         3415554 ops/sec           402 B/op         16 allocs/op
func BenchmarkServer_SetCommand(b *testing.B) {
	_ = os.Remove(sockfilePath)

	ln, err := net.Listen("unix", sockfilePath)
	if err != nil {
		b.Fatal(err)
	}

	server := sider.NewServer(ln)

	go func() {
		err = server.Start()
		if err != nil && !errors.Is(err, net.ErrClosed) {
			b.Errorf("server error: %s", err.Error())
		}
	}()

	b.ResetTimer()

	id := atomic.Int64{}
	b.RunParallel(func(pb *testing.PB) {
		client, err := net.Dial("unix", sockfilePath)
		if err != nil {
			b.Error(err)
			return
		}

		randomizer := rand.New(rand.NewSource(id.Add(1)))

		pipelineSize := 1000

		buff := make([]byte, 40960)
		writeBuffer := bytes.Buffer{}
		count := 0
		for pb.Next() {
			writeBuffer.WriteString("*3\r\n$3\r\nset\r\n$12\r\n")
			for i := 0; i < 12; i++ {
				writeBuffer.WriteByte(byte(randomizer.Int31()%96 + 32))
			}
			writeBuffer.WriteString("\r\n$12\r\n")
			for i := 0; i < 12; i++ {
				writeBuffer.WriteByte(byte(randomizer.Int31()%96 + 32))
			}
			writeBuffer.WriteString("\r\n")
			count++

			if count >= pipelineSize {
				if _, err := writeBuffer.WriteTo(client); err != nil {
					b.Errorf("cannot write to server: %s", err.Error())
					return
				}

				if _, err := io.ReadFull(client, buff[:5*count]); err != nil {
					b.Errorf("cannot read from server: %s", err.Error())
					return
				}

				count = 0
			}
		}

		if count > 0 {
			if _, err := writeBuffer.WriteTo(client); err != nil {
				b.Errorf("cannot write to server: %s", err.Error())
				return
			}

			if _, err := io.ReadFull(client, buff[:5*count]); err != nil {
				b.Errorf("cannot read from server: %s", err.Error())
				return
			}

			count = 0
		}

		if err := client.Close(); err != nil {
			b.Errorf("cannot close client: %s", err.Error())
			return
		}
	})
	b.StopTimer()

	err = server.Stop()
	if err != nil {
		b.Errorf("server stop error: %s", err.Error())
		return
	}

	throughput := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(throughput, "ops/sec")
}
