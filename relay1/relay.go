package relay1

import (
	"context"
	"errors"
	"github.com/jack0son/relays/pubsub"
)

var SourceClosed = errors.New("source channel closed")
var BusDone = errors.New("relay bus closed")
var ErrUnknownEvent = errors.New("incompatible event for source")

// Relay forwards results from a channel to a bus. It makes the error
// from the relaying function, passing it to the provided callback
type Relay struct {
	pubsub.Bus
	ctx   context.Context
	errCh chan error
}

func NewRelay(ctx context.Context) Relay {
	return Relay{
		Bus:   pubsub.NewBus(),
		ctx:   ctx,
		errCh: make(chan error),
	}
}

func NewRelayWithErrChan(ctx context.Context, errCh chan error) Relay {
	return Relay{
		Bus:   pubsub.NewBus(),
		ctx:   ctx,
		errCh: errCh,
	}
}

// TriggerFunc
type TriggerFunc func() error

type CatchCallback = func(error, ...interface{})

// Relayer relies too much on the implentation of the trigger func
// - would like to be able to pass a middleware and generically
// process pipline: read -> process -> publish
func (r *Relay) Start(runRelay TriggerFunc) {
	// @todo should relayer use its own loop to allow the relay function to continue after failing
	// OR should an error in the relay fn be fatal to the relay

	// receive event -> process -> publish

	go func() {
		// This blocks on receive, unless error is consumed, go routine will leak
		r.errCh <- runRelay()
	}()

	// relay.Source.Forward()
}

func (r *Relay) Catch(cb CatchCallback) {
	go func() {
		var err error
	loop:
		for {
			_err, ok := <-r.errCh
			if _err != nil {
				err = _err
			}
			if !ok {
				r.errCh = nil
			}
			break loop
		}

		if cb != nil {
			cb(err)
		}
	}()
}
