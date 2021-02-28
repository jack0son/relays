package relay2

import (
	"context"
	"fmt"
	"github.com/jack0son/relays/pubsub"
	"github.com/jack0son/relays/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"sync"
	"testing"
	"time"
)

func getNewRelay() (Relay, chan testEvent, context.Context,context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	relay := NewRelay(ctx)
	events := make(chan testEvent, 100)

	return relay, events, ctx, cancel
}

func TestRelay(t *testing.T) {
	relay, events, _, _ := getNewRelay()
	defer goleak.VerifyNone(t)

	gen := test.RandomIDGenerator()
	ev := newEvent([]byte(gen()))

	wg := sync.WaitGroup{}
	source := newTestSource(events, &wg)

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

	relay.Run(source)
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

func TestRelayBusClosed(t *testing.T) {
	relay, events, _, _ := getNewRelay()
	defer goleak.VerifyNone(t)

	gen := test.RandomIDGenerator()
	ev := newEvent([]byte(gen()))

	wg := sync.WaitGroup{}
	source := newTestSource(events, &wg)

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

	relay.Run(source)
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
			if i == len(evs) -1 {
				relay.Close()
			}
			if i >= len(evs) {
				break loop
			}
		case <-sub.Done():
			break loop
		}
	}

	wg.Wait()
}

func TestRelayContextCanceled(t *testing.T) {
	relay, events, _, cancel := getNewRelay()
	defer goleak.VerifyNone(t)

	gen := test.RandomIDGenerator()
	ev := newEvent([]byte(gen()))

	wg := sync.WaitGroup{}
	source := newTestSource(events, &wg)

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

	wg2 := sync.WaitGroup{}
	wg2.Add(1)
	relay.RunWithCb(source, func (err error) {
		relay.coupledShutdownFn(source)(err)
		wg2.Done()
	})
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
			if i == len(evs) - 2 {
				cancel()
			}
			if i >= len(evs) {
				break loop
			}
		case <-sub.Done():
			break loop
		}
	}

	time.Sleep(time.Second * 2)
	//wg2.Wait()
}

func newTestSource(in chan testEvent, wg *sync.WaitGroup) testSource {
	wg.Add(1)
	return testSource {
		in: in,
		out: make(chan interface{}, 1),
		wg: wg,
	}
}

type testSource struct {
	in chan testEvent
	out chan interface{}
	wg *sync.WaitGroup
}

func (s testSource) Read(done <-chan struct{}) <-chan interface{} {
	select {
		case s.out<- <-s.in:
		case <-done:
			fmt.Println("source canceled")
	}
	return s.out
}

func (s testSource) Close()  {
	close(s.in)
	s.wg.Done()
}

func (s testSource) Publish(event pubsub.Event, bus pubsub.Bus) error {
	switch evt := event.(type) {
	case testEvent:
		// middleware goes here
		err := bus.Publish(evt)
		if err != nil {
			return err
		}
	default:
		return ErrUnknownEvent
	}

	return nil
}

type testEvent []byte

func newEvent(addr []byte) testEvent {
	return testEvent(addr)
}
