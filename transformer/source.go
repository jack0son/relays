package transformer

import (
	"context"
	"github.com/jack0son/relays/test"
	"golang.org/x/sync/errgroup"
)

type EventSource struct {
	events      chan test.TestEvent
}

func NewSource(events chan test.TestEvent) EventSource {
	return EventSource{events: events}
}

func (es EventSource) Close() {
	close(es.events)
}

func (es EventSource) Start(ctx context.Context, eg *errgroup.Group, sinkFunc SinkFunc) {
	// Start the parse stage and return the sink stage
	transformed := transformTestEvents(ctx, eg, es.events)
	sinkParseEvents(ctx, eg, transformed)(sinkFunc)
}

func transformTestEvents(ctx context.Context, eg *errgroup.Group, inCh <-chan test.TestEvent) <-chan ParsedEvent {
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

func sinkParseEvents(ctx context.Context, eg *errgroup.Group, inCh <-chan ParsedEvent) SourceSinkFunc {
	return func(sinkFunc SinkFunc) {
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
