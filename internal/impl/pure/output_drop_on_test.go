package pure_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v3"

	"github.com/usedatabrew/benthos/v4/internal/component/output"
	bmock "github.com/usedatabrew/benthos/v4/internal/manager/mock"
	"github.com/usedatabrew/benthos/v4/internal/message"

	_ "github.com/usedatabrew/benthos/v4/public/components/pure"
)

func parseYAMLOutputConf(t testing.TB, formatStr string, args ...any) (conf output.Config) {
	t.Helper()
	conf = output.NewConfig()
	require.NoError(t, yaml.Unmarshal(fmt.Appendf(nil, formatStr, args...), &conf))
	return
}

func TestDropOnNothing(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "test error", http.StatusForbidden)
	}))
	t.Cleanup(func() {
		ts.Close()
	})

	childConf := parseYAMLOutputConf(t, `
http_client:
  url: %v
  drop_on: [ %v ]
`, ts.URL, http.StatusForbidden)

	dropConf := output.NewConfig()
	dropConf.Type = "drop_on"
	dropConf.DropOn.Error = false
	dropConf.DropOn.Output = &childConf

	d, err := bmock.NewManager().NewOutput(dropConf)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		d.TriggerCloseNow()
		assert.NoError(t, d.WaitForClose(ctx))
		done()
	})

	tChan := make(chan message.Transaction)
	rChan := make(chan error)

	require.NoError(t, d.Consume(tChan))

	select {
	case tChan <- message.NewTransaction(message.QuickBatch([][]byte{[]byte("foobar")}), rChan):
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	var res error
	select {
	case res = <-rChan:
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	assert.EqualError(t, res, fmt.Sprintf("%s: HTTP request returned unexpected response code (403): 403 Forbidden, Error: test error", ts.URL))
}

func TestDropOnError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "test error", http.StatusForbidden)
	}))
	t.Cleanup(func() {
		ts.Close()
	})

	childConf := parseYAMLOutputConf(t, `
http_client:
  url: %v
  drop_on: [ %v ]
`, ts.URL, http.StatusForbidden)

	dropConf := output.NewConfig()
	dropConf.Type = "drop_on"
	dropConf.DropOn.Error = true
	dropConf.DropOn.Output = &childConf

	d, err := bmock.NewManager().NewOutput(dropConf)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		d.TriggerCloseNow()
		assert.NoError(t, d.WaitForClose(ctx))
		done()
	})

	tChan := make(chan message.Transaction)
	rChan := make(chan error)

	require.NoError(t, d.Consume(tChan))

	select {
	case tChan <- message.NewTransaction(message.QuickBatch([][]byte{[]byte("foobar")}), rChan):
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	var res error
	select {
	case res = <-rChan:
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	assert.NoError(t, res)
}

func TestDropOnBackpressureWithErrors(t *testing.T) {
	// Skip this test in most runs as it relies on awkward timers.
	t.Skip()

	var wsMut sync.Mutex
	var wsReceived []string
	var wsAllow bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wsMut.Lock()
		allow := wsAllow
		wsMut.Unlock()
		if !allow {
			http.Error(w, "nope", http.StatusForbidden)
			return
		}

		upgrader := websocket.Upgrader{}

		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()

		for {
			_, actBytes, err := ws.ReadMessage()
			if err != nil {
				return
			}
			wsMut.Lock()
			wsReceived = append(wsReceived, string(actBytes))
			wsMut.Unlock()
		}
	}))
	t.Cleanup(func() {
		ts.Close()
	})

	dropConf := parseYAMLOutputConf(t, `
drop_on:
  back_pressure: 100ms
  output:
    websocket:
      url: %v
`, "ws://"+strings.TrimPrefix(ts.URL, "http://"))

	d, err := bmock.NewManager().NewOutput(dropConf)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		d.TriggerCloseNow()
		assert.NoError(t, d.WaitForClose(ctx))
		done()
	})

	tChan := make(chan message.Transaction)
	rChan := make(chan error)

	require.NoError(t, d.Consume(tChan))

	sendAndGet := func(msg, expErr string) {
		t.Helper()

		select {
		case tChan <- message.NewTransaction(message.QuickBatch([][]byte{[]byte(msg)}), rChan):
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}

		var res error
		select {
		case res = <-rChan:
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}

		if expErr == "" {
			assert.NoError(t, res)
		} else {
			assert.EqualError(t, res, expErr)
		}
	}

	sendAndGet("first", "experienced back pressure beyond: 100ms")
	sendAndGet("second", "experienced back pressure beyond: 100ms")
	wsMut.Lock()
	wsAllow = true
	wsMut.Unlock()
	<-time.After(time.Second)

	sendAndGet("third", "")
	sendAndGet("fourth", "")

	<-time.After(time.Second)
	wsMut.Lock()
	assert.Equal(t, []string{"third", "fourth"}, wsReceived)
	wsMut.Unlock()
}

func TestDropOnDisconnectBackpressureNoErrors(t *testing.T) {
	// Skip this test in most runs as it relies on awkward timers.
	t.Skip()

	var wsReceived []string
	var ws *websocket.Conn
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}

		var err error
		if ws, err = upgrader.Upgrade(w, r, nil); err != nil {
			return
		}
		defer ws.Close()

		for {
			_, actBytes, err := ws.ReadMessage()
			if err != nil {
				return
			}
			wsReceived = append(wsReceived, string(actBytes))
		}
	}))
	t.Cleanup(func() {
		ts.Close()
	})

	dropConf := parseYAMLOutputConf(t, `
drop_on:
  back_pressure: 100ms
  error: true
  output:
    websocket:
      url: %v
`, "ws://"+strings.TrimPrefix(ts.URL, "http://"))

	d, err := bmock.NewManager().NewOutput(dropConf)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		d.TriggerCloseNow()
		assert.NoError(t, d.WaitForClose(ctx))
		done()
	})

	tChan := make(chan message.Transaction)
	rChan := make(chan error)

	require.NoError(t, d.Consume(tChan))

	sendAndGet := func(msg, expErr string) {
		t.Helper()

		select {
		case tChan <- message.NewTransaction(message.QuickBatch([][]byte{[]byte(msg)}), rChan):
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}

		var res error
		select {
		case res = <-rChan:
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}

		if expErr == "" {
			assert.NoError(t, res)
		} else {
			assert.EqualError(t, res, expErr)
		}
	}

	sendAndGet("first", "")
	sendAndGet("second", "")

	ts.Close()
	ws.Close()
	<-time.After(time.Second)

	sendAndGet("third", "")
	sendAndGet("fourth", "")

	<-time.After(time.Second)

	assert.Equal(t, []string{"first", "second"}, wsReceived)
}
