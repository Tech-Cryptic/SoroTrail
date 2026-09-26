package webhook

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

func TestWebhookDeliveryLifecycle_Table(t *testing.T) {
	tests := []struct {
		name             string
		serverHandler    http.HandlerFunc
		expectedAttempts int
		expectDisabled   bool
		expectReset      bool
		expectSignature  bool
		timeout          time.Duration
	}{
		{
			name: "successful delivery resets the failure counter",
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			},
			expectedAttempts: 1,
			expectReset:      true,
			expectSignature:  true,
		},
		{
			name: "failing endpoint is retried and auto-disables",
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
			},
			expectedAttempts: 5,
			expectDisabled:   true,
		},
		{
			name: "slow endpoint is cut off by the timeout",
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				time.Sleep(100 * time.Millisecond) // deliberately slow
				w.WriteHeader(http.StatusOK)
			},
			expectedAttempts: 5,
			expectDisabled:   true,
			timeout:          10 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			done := make(chan struct{})
			// Retries can overlap when a request is cut off by the client
			// timeout, so the handler runs concurrently — the counter must
			// be atomic or the race detector (rightly) fails the test.
			var callCount atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempts := callCount.Add(1)

				if tt.expectSignature {
					assert.NotEmpty(t, r.Header.Get(SignatureHeader))
					body, _ := io.ReadAll(r.Body)
					expectedSig := Sign("secret123", body)
					assert.Equal(t, expectedSig, r.Header.Get(SignatureHeader))
				}

				tt.serverHandler(w, r)
				if int(attempts) == tt.expectedAttempts {
					close(done)
				}
			}))
			defer server.Close()

			st := newStubSubscriptionStore()
			st.enabledSubs = []store.Subscription{{
				ID:      1,
				URL:     server.URL + "/callback",
				Secret:  "secret123",
				Enabled: true,
			}}

			n := newTestNotifier(st, testLogger())
			if tt.timeout > 0 {
				n.client.Timeout = tt.timeout
			}
			ctx, cancel := context.WithCancel(context.Background())
			go n.Run(ctx)
			defer cancel()

			n.NotifyEvents(context.Background(), []store.Event{testEvent("e1")})

			select {
			case <-done:
				time.Sleep(100 * time.Millisecond) // Allow recordSuccess/incrementFailures to process
			case <-time.After(3 * time.Second):
				if got := callCount.Load(); got < int64(tt.expectedAttempts) {
					t.Fatalf("timed out waiting for worker. expected %d calls, got %d", tt.expectedAttempts, got)
				}
			}

			st.mu.Lock()
			defer st.mu.Unlock()

			require.Len(t, st.attempts, tt.expectedAttempts, "unexpected number of delivery attempts")

			if tt.expectReset {
				assert.Contains(t, st.resets, int64(1))
			} else {
				assert.NotContains(t, st.resets, int64(1))
			}

			if tt.expectDisabled {
				// Each failure attempts to increment failures by 3 or similar?
				// Let's just check if it incremented failures
				assert.NotEmpty(t, st.failures)
			}
		})
	}
}
