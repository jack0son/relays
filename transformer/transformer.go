package transformer

import (
	"context"
	"fmt"
	"github.com/jack0son/relays/pubsub"
	"github.com/jack0son/relays/test"
	"golang.org/x/sync/errgroup"
)

type ParsedEvent = test.TestEvent

var empty ParsedEvent
func parseEvent(in test.TestEvent) (ParsedEvent, error) {
	p := append([]byte(in), []byte("parsed")...)
	if in == nil  {
		fmt.Println(p)
	}
	return ParsedEvent(test.NewEvent(p)), nil
}

func parser(ctx context.Context, eg *errgroup.Group, inCh <-chan test.TestEvent) <-chan ParsedEvent {
	outCh := make(chan ParsedEvent)
	eg.Go(func() error {
		defer close(outCh)
		for ev := range inCh {
			parsed, err := parseEvent(ev)
			if err != nil {
				// Handle an error that occurs during the goroutine.
				return err
			}
			// Send the data to the output channel but return early
			// if the context has been cancelled.
			select {
			case outCh <- parsed:
			case <-ctx.Done():
				return nil
			}
		}
		return nil
	})
	return outCh
}

func sinkSelect(ctx context.Context, eg *errgroup.Group, inCh <-chan ParsedEvent, sinkFunc func(interface{}) error ) {
	eg.Go(func() error {
		for {
			select{
			case ev, ok := <-inCh:
				if !ok { // if in channel closes, turn off
					return nil
				}

				err := sinkFunc(ev)
				if err != nil {
					return err
				}
			case <-ctx.Done():
				return nil
			}
		}
	})
}

func sink(ctx context.Context, eg *errgroup.Group, inCh <-chan ParsedEvent, sinkFunc func(interface{}) error ) {
	eg.Go(func() error {
			for ev := range inCh {
				err := sinkFunc(ev)
				if err != nil {
					return err
				}
			}
			return nil
	})
}

func sinkForRelayFn(inCh <-chan ParsedEvent) SinkStageFunc {
	return func(ctx context.Context, eg *errgroup.Group, sinkFunc SinkFunc) {
		eg.Go(func() error {
			for ev := range inCh {
				err := sinkFunc(ev)
				if err != nil {
					return err
				}
			}
			return nil
		})
	}
}

type Source interface {
	Close()
	Start(context.Context, *errgroup.Group, SinkFunc)
}

// SinkStageFunc is the final stage of a pipeline
type SinkStageFunc = func(context.Context, *errgroup.Group, SinkFunc)
type SourceSinkFunc = func(SinkFunc)
type SinkFunc = func(pubsub.Event) error

type Relay struct {
	pubsub.Bus
	source Source

	ctx context.Context
	cancel context.CancelFunc
	eg *errgroup.Group
}

func NewRelay(ctx context.Context, source Source) Relay {
	ctx, cancel := context.WithCancel(ctx)
	eg, ctx := errgroup.WithContext(ctx)

	return Relay{
		Bus: pubsub.NewBus(),
		source: source,
		ctx: ctx,
		cancel: cancel,
		eg: eg,
	}
}

func (r Relay) Start() {
	r.source.Start(r.ctx, r.eg, r.Publish)
}

func (r Relay) Close() {
	r.cancel()
	r.Bus.Close()
	r.source.Close()
}

func (r Relay) ErrGroup() *errgroup.Group {
	return r.eg
}