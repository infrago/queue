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
