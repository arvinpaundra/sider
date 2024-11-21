package resp_test

import (
	"strings"
	"testing"

	"github.com/arvinpaundra/sider/internal/resp"
)

func TestReadRESP(t *testing.T) {
	input := strings.NewReader("*3\r\n$5\r\nword1\r\n$5\r\nword2\r\n$5\r\nword3\r\n")
	reader := resp.NewReader(input)
	req, err := reader.Read()

	if err != nil {
		t.Errorf("failed to parse input: %s", err.Error())
		t.FailNow()
	}

	if len(req.Values) != 3 {
		t.Errorf("values size not satisfied with expected 3, but got %d", len(req.Values))
		t.FailNow()
	}

	val0 := req.Values[0].Str
	if val0 != "word1" {
		t.Errorf("val[0] is not 'word1', but got %s", val0)
		t.FailNow()
	}

	val1 := req.Values[1].Str
	if val1 != "word2" {
		t.Errorf("val[1] is not 'word2', but got %s", val1)
		t.FailNow()
	}

	val2 := req.Values[2].Str
	if val2 != "word3" {
		t.Errorf("val[2] is not 'word3', but got %s", val2)
		t.FailNow()
	}
}
