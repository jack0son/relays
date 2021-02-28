package transformer

import (
	"context"
	"fmt"
	"github.com/jack0son/relays/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"golang.org/x/sync/errgroup"
	"testing"
)

//func getNewRelay() (Relay, chan testEvent, context.Context,context.CancelFunc) {
//	ctx, cancel := context.WithCancel(context.Background())
//	relay := NewRelay(ctx)
//	events := make(chan testEvent, 100)
//
//	return relay, events, ctx, cancel
//}

func TestPipeline(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx, cancel := context.WithCancel(context.Background())
	eg, ctx := errgroup.WithContext(ctx)

	gen := test.RandomIDGenerator()
	ev := test.NewEvent([]byte(gen()))
	events := make(chan test.TestEvent, 10)

	finish := func() {
		close(events)
		cancel()
	}

	const power = 3
	evs := []test.TestEvent{ev}
	for i := 0; i < power; i++ {
		evs = append(evs, evs...)
	}

	done := make(chan struct{})

	i := 0
	sinkFunc := func(v interface{}) error {
		fmt.Println("sink", v)
		i++
		if i == len(evs) {
			close(done)
		}
		if i > len(evs) {
			assert.Fail(t, "additional events received")
		}
		return nil
	}

	parsedCh := parser(ctx, eg, events)
	sink(ctx, eg, parsedCh, sinkFunc)

	go func() {
		for _, ev := range evs {
			events <- ev
		}
	}()

	<-done

	finish()
	//err := eg.Wait()
	//require.NoError(t, err)
}

func TestRelay(t *testing.T) {
	defer goleak.VerifyNone(t)

	gen := test.RandomIDGenerator()
	ev := test.NewEvent([]byte(gen()))
	events := make(chan test.TestEvent, 10)

	ctx, _ := context.WithCancel(context.Background())
	source := NewSource(events)
	relay := NewRelay(ctx, source)

	relay.Start()

	const power = 3
	evs := []test.TestEvent{ev}
	for i := 0; i < power; i++ {
		evs = append(evs, evs...)
	}

	go func() {
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
			fmt.Printf("%s\n", newEv)
			assert.Equal(t, test.NewEvent([]byte(string(ev) + "parsed")), newEv)
			i++
			if i == len(evs) - 2 {
				//relay.Close()
			}
			if i >= len(evs) {
				break loop
			}
		case <-sub.Done():
			break loop
		}
	}

	relay.Close()
	err = relay.ErrGroup().Wait()
	require.NoError(t, err)
}
