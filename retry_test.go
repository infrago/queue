package queue

import (
	"testing"
	"time"
)

func TestQueueFinal(t *testing.T) {
	cases := []struct {
		retryCount int
		attempt    int
		final      bool
	}{
		{retryCount: 0, attempt: 1, final: false},
		{retryCount: 3, attempt: 1, final: false},
		{retryCount: 3, attempt: 2, final: false},
		{retryCount: 3, attempt: 3, final: false},
		{retryCount: 3, attempt: 4, final: true},
		{retryCount: 3, attempt: 0, final: false},
	}
	for _, c := range cases {
		if got := queueFinal(c.retryCount, c.attempt); got != c.final {
			t.Fatalf("retryCount=%d attempt=%d final=%v got=%v", c.retryCount, c.attempt, c.final, got)
		}
	}
}

func TestNextRetryDelay(t *testing.T) {
	inst := &Instance{}
	ctx := &Context{
		Config: &Queue{Retry: []time.Duration{3 * time.Second, 10 * time.Second, 30 * time.Second}},
	}

	cases := []struct {
		attempt int
		delay   time.Duration
	}{
		{attempt: 1, delay: 3 * time.Second},
		{attempt: 2, delay: 10 * time.Second},
		{attempt: 3, delay: 30 * time.Second},
		{attempt: 4, delay: 30 * time.Second},
		{attempt: 0, delay: 3 * time.Second},
	}

	for _, c := range cases {
		ctx.attempt = c.attempt
		if got := inst.nextRetryDelay(ctx); got != c.delay {
			t.Fatalf("attempt=%d delay=%v got=%v", c.attempt, c.delay, got)
		}
	}
}

func TestResponseMetaDoesNotRetryAfterFinalAttempt(t *testing.T) {
	inst := &Instance{}
	ctx := &Context{
		Config:  &Queue{Retry: []time.Duration{time.Second}},
		Body:    retryBody{},
		attempt: 2,
		final:   true,
	}

	retry, delay := inst.responseMeta(ctx)
	if retry || delay != 0 {
		t.Fatalf("retry=%v delay=%v, want no retry", retry, delay)
	}
}

func TestDefaultConnectionKeepsThreadRegistrations(t *testing.T) {
	conn := &defaultConnection{
		queues: make(map[string]chan *defaultMessage),
		names:  make([]string, 0),
		done:   make(chan struct{}),
	}

	if err := conn.Register("jobs"); err != nil {
		t.Fatal(err)
	}
	if err := conn.Register("jobs"); err != nil {
		t.Fatal(err)
	}

	if len(conn.queues) != 1 {
		t.Fatalf("queues=%d, want 1", len(conn.queues))
	}
	if len(conn.names) != 2 {
		t.Fatalf("workers=%d, want 2", len(conn.names))
	}
}

func TestModuleStartRollsBackStartedConnections(t *testing.T) {
	state := &testStartState{failOn: 2}
	first := &testConnection{state: state}
	second := &testConnection{state: state}
	mod := &Module{
		instances: map[string]*Instance{
			"one": {conn: first},
			"two": {conn: second},
		},
	}

	defer func() {
		if recover() == nil {
			t.Fatal("expected Start to panic")
		}
		started := 0
		stopped := 0
		for _, conn := range []*testConnection{first, second} {
			if conn.started {
				started++
				if !conn.stopped {
					t.Fatal("started connection was not stopped during rollback")
				}
			}
			if conn.stopped {
				stopped++
			}
		}
		if started != 1 || stopped != 1 {
			t.Fatalf("started=%d stopped=%d, want one started connection to be rolled back", started, stopped)
		}
	}()

	mod.Start()
}

type testConnection struct {
	state   *testStartState
	started bool
	stopped bool
}

type testStartState struct {
	count  int
	failOn int
}

func (c *testConnection) Open() error                                         { return nil }
func (c *testConnection) Close() error                                        { return nil }
func (c *testConnection) Register(string) error                               { return nil }
func (c *testConnection) Publish(string, []byte) error                        { return nil }
func (c *testConnection) DeferredPublish(string, []byte, time.Duration) error { return nil }

func (c *testConnection) Start() error {
	if c.state != nil {
		c.state.count++
		if c.state.count == c.state.failOn {
			return errQueueRunning
		}
	}
	c.started = true
	return nil
}

func (c *testConnection) Stop() error {
	c.stopped = true
	return nil
}
