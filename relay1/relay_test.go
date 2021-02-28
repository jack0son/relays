package relay1

import (
	"context"
	"github.com/jack0son/relays/pubsub"
	"github.com/jack0son/relays/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"sync"
	"testing"
)

func TestRelay(t *testing.T) {
	ctx, relay, events, _ := getNewRelay()
	defer goleak.VerifyNone(t)

	gen := test.RandomIDGenerator()
	ev := newEvent([]byte(gen()))

	wg := sync.WaitGroup{}
	relay.Catch(func(err error, args ...interface{}) {
		assert.Equal(t, err, BusDone)
		wg.Done()
	})
	relay.Start(func() error {
		wg.Add(1)
		return runRelay(ctx, events, relay.Bus)
	})

	const power = 3
	evs := []testEvent{ev}
	for i := 0; i < power; i++ {
		evs = append(evs, evs...)
	}

	go func() {
		wg.Add(1)
		defer wg.Done()

		for _, ev := range evs {
			events <- ev
		}
	}()

	sub, err := relay.Subscribe()
	require.NoError(t, err)

	i := 0
loop:
	for {
		select {
		case newEv, ok := <-sub.Events():
			if !ok {
				assert.Fail(t, "bus channel closed")
			}
			assert.Equal(t, ev, newEv)
			i++
			if i >= len(evs) {
				break loop
			}
		}
	}

	relay.Close()
	wg.Wait()
}

//func TestRelayContextCanceled(t *testing.T) {
//	ctx, relay, events, cancel := getNewRelay()
//	defer goleak.VerifyNone(t)
//
//	did := ed25519.GenPrivKey().PubKey().Address()
//	ev := newEvent(did)
//
//	relay.Catch(func(err error, args ...interface{}) {
//		assert.Equal(t, DoneContext{}, args[0])
//	})
//	relay.Start(func() error {
//		return runRelay(ctx, events, relay)
//	})
//
//	events <- ev
//	events <- ev
//
//	// Subscribe to bus to
//	sub, err := relay.Subscribe()
//	require.NoError(t, err)
//
//	i := 0
//	select {
//	case <-sub.Events():
//		i++
//		if i > 0 {
//			cancel()
//		}
//	}
//}

func TestRelayBusClosed(t *testing.T) {
}

func TestCloseBusToCloseSource(t *testing.T) {
}

func TestRelaySourceClosed(t *testing.T) {
	ctx, relay, events, _ := getNewRelay()
	defer goleak.VerifyNone(t)

	gen := test.RandomIDGenerator()
	ev := newEvent([]byte(gen()))

	wg := sync.WaitGroup{}
	relay.Catch(func(err error, args ...interface{}) {
		require.Equal(t, SourceClosed, err)
		wg.Done()
	})
	relay.Start(func() error {
		wg.Add(1)
		return runRelay(ctx, events, relay.Bus)
	})

	const power = 4
	evs := []testEvent{ev}
	for i := 0; i < power; i++ {
		evs = append(evs, evs...)
	}

	go func() {
		wg.Add(1)
		defer wg.Done()

		for _, ev := range evs {
			events <- ev
		}
		close(events)
	}()

	sub, err := relay.Subscribe()
	require.NoError(t, err)

	i := 0
loop:
	for {
		select {
		case newEv, ok := <-sub.Events():
			if !ok {
				assert.Fail(t, "bus channel closed")
			}
			assert.Equal(t, ev, newEv)
			i++
			if i >= len(evs) {
				break loop
			}
		case <-sub.Done():
			break loop
		}
	}

	wg.Wait()
	relay.Close() // close bus go routine
}

// Create a fake tm event server and test bus relays
func getNewRelay() (context.Context, Relay, chan testEvent, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	relay := NewRelay(ctx)
	events := make(chan testEvent, 100)

	return ctx, relay, events, cancel
}

type eventSource struct {
	msgCh chan testEvent
}

func (e eventSource) ForwardA(publish func(event pubsub.Event)) {
	for ev := range e.msgCh {
		publish(ev)
	}
}

// Forward can be generic instead of receiving from the  event chan
// - then make relay trigger catch the processing error
func (e eventSource) Forward(bus pubsub.Bus, process func(pubsub.Bus, interface{}) error) {
	for ev := range e.msgCh {
		process(bus, ev)
	}
}


// maybe the relayer could return a function for processing?
// how can we switch on the different channels generically?

// If context is closed, bus should be closed by relayer
func runRelay(ctx context.Context, source <-chan testEvent, sink pubsub.Bus) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sink.Done():
			return BusDone
		case msg, ok := <-source:
			if !ok {
				return SourceClosed
			}

			err := sink.Publish(msg)
			if err != nil {
				return err
			}
		}

	}
}

type testEvent []byte

func newEvent(addr []byte) testEvent {
	return testEvent(addr)
}
