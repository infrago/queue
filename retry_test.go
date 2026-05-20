package queue

import (
	"testing"
	"time"

	"github.com/infrago/infra"
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
		queues:  make(map[string]chan *defaultMessage),
		workers: make(map[string]int),
		timers:  make(map[*time.Timer]struct{}),
		done:    make(chan struct{}),
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
	if conn.workers["jobs"] != 2 {
		t.Fatalf("workers=%d, want 2", conn.workers["jobs"])
	}
}

func TestDefaultConnectionPublishReturnsWhenQueueIsFull(t *testing.T) {
	conn := &defaultConnection{
		queues:  make(map[string]chan *defaultMessage),
		workers: make(map[string]int),
		timers:  make(map[*time.Timer]struct{}),
		done:    make(chan struct{}),
	}
	if err := conn.Register("jobs"); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < cap(conn.queues["jobs"]); i++ {
		if err := conn.Publish("jobs", nil); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	if err := conn.Publish("jobs", nil); err != errQueueFull {
		t.Fatalf("publish full error=%v, want %v", err, errQueueFull)
	}
}

func TestDefaultConnectionUsesConfiguredBuffer(t *testing.T) {
	conn := &defaultConnection{
		instance: &Instance{Setting: map[string]any{"buffer": 3}},
		queues:   make(map[string]chan *defaultMessage),
		workers:  make(map[string]int),
		timers:   make(map[*time.Timer]struct{}),
		done:     make(chan struct{}),
	}
	if err := conn.Register("jobs"); err != nil {
		t.Fatal(err)
	}
	if cap(conn.queues["jobs"]) != 3 {
		t.Fatalf("buffer=%d, want 3", cap(conn.queues["jobs"]))
	}
}

func TestDefaultConnectionPublishWaitSettings(t *testing.T) {
	conn := &defaultConnection{
		instance: &Instance{Setting: map[string]any{
			"blocking_publish": "true",
			"publish_timeout":  "25ms",
		}},
	}
	wait, timeout := conn.publishWait()
	if !wait || timeout != 25*time.Millisecond {
		t.Fatalf("wait=%v timeout=%v", wait, timeout)
	}
}

func TestDeadLetterName(t *testing.T) {
	ctx := &Context{Config: &Queue{Setting: map[string]any{"dead_letter": "jobs.dead"}}}
	if got := deadLetterName(ctx); got != "jobs.dead" {
		t.Fatalf("dead=%q", got)
	}
	if !shouldDeadLetter(ctx, infra.Fail) {
		t.Fatal("expected failed result to dead letter")
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

func TestModuleCloseStopsRunningConnections(t *testing.T) {
	conn := &testConnection{}
	mod := &Module{
		opened:  true,
		started: true,
		instances: map[string]*Instance{
			"default": {conn: conn},
		},
	}

	mod.Close()

	if !conn.stopped || !conn.closed {
		t.Fatalf("stopped=%v closed=%v, want both", conn.stopped, conn.closed)
	}
	if mod.started || mod.opened {
		t.Fatalf("started=%v opened=%v, want false", mod.started, mod.opened)
	}
}

type testConnection struct {
	state   *testStartState
	started bool
	stopped bool
	closed  bool
}

type testStartState struct {
	count  int
	failOn int
}

func (c *testConnection) Open() error { return nil }
func (c *testConnection) Close() error {
	c.closed = true
	return nil
}
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
