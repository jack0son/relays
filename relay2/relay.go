package relay2

import (
	"context"
	"errors"
	"fmt"
	"github.com/jack0son/relays/pubsub"
)

var SourceClosed = errors.New("source channel closed")
var BusDone = errors.New("relay bus closed")
var ErrUnknownEvent = errors.New("incompatible event for source")

// Source is a wrapper around a subscription channel
// Have to have this generic interface to make relays generalise to arbitrary channels
type Source interface {
	Read(<-chan struct{}) <-chan interface{}
	Publish(event pubsub.Event, bus pubsub.Bus) error
	Close()
}

type Relay struct {
	pubsub.Bus
	errCh chan error
	ctx   context.Context
	cancel context.CancelFunc
}

func (r *Relay) Run(source Source) {
	r.RunWithCb(source, r.coupledShutdownFn(source))
}

type CatchFunc = func(error)

var i int = 0
func (r *Relay) RunWithCb(source Source, cb CatchFunc) {
	done := make(chan struct {})
	go func() {
		for {
			i += 1
			fmt.Println("run ", i)
			select {
			case <-r.ctx.Done():
				close(done)
				r.errCh <- r.ctx.Err()
				return
			case <-r.Done():
				close(done)
				r.errCh <- BusDone
				return
			case msg, ok := <-source.Read(done):
				if !ok {
					r.errCh <- SourceClosed
					return
				}

				err := source.Publish(msg, r)
				if err != nil {
					r.errCh <- err
					return
				}
			}
		}
	}()

	go r.catch(cb)
}

func (r *Relay) coupledShutdownFn(source Source) CatchFunc {
	return func(err error) {
		switch err {
		case SourceClosed:
			//r.cancel()
			r.Close()
		case BusDone:
			fallthrough
		case pubsub.ErrNotRunning:
			//r.cancel()
			source.Close()
		case context.Canceled:
			r.Close()
			source.Close()
		default:
			fmt.Printf("unknown shutdown: %s\n", err)
		}
	}
}

func NewRelay(ctx context.Context) Relay {
	ctx, cancel := context.WithCancel(ctx)
	return Relay{
		Bus:   pubsub.NewBus(),
		ctx:   ctx,
		cancel: cancel,
		errCh: make(chan error),
	}
}

func (r *Relay) catch(cb CatchFunc) {
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

	cb(err)
}
