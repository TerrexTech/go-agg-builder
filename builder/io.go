package builder

import (
	"context"
	"sync"

	"github.com/TerrexTech/uuuid"
	"github.com/pkg/errors"

	"golang.org/x/sync/errgroup"
)

// EventsIO allows interacting with the AggBuilder service.
// This is the medium through which the new events are distributed.
type EventsIO struct {
	closeChan   chan<- struct{}
	errGroup    *errgroup.Group
	errGroupCtx context.Context

	ctxOpen bool
	ctxLock sync.RWMutex

	esReqProd *esReqProducer
	eventResp chan *EventResponse
	waitChan  chan error
}

func newEventsIO(
	errGroupCtx context.Context,
	errGroup *errgroup.Group,
	closeChan chan<- struct{},
	esReqProd *esReqProducer,
) *EventsIO {
	return &EventsIO{
		closeChan:   closeChan,
		errGroup:    errGroup,
		errGroupCtx: errGroupCtx,

		ctxOpen: true,
		ctxLock: sync.RWMutex{},

		esReqProd: esReqProd,
		eventResp: make(chan *EventResponse, 256),
	}
}

// RoutinesGroup returns the errgroup used for AggBuilder-routines.
func (e *EventsIO) RoutinesGroup() *errgroup.Group {
	return e.errGroup
}

// Context returns the errgroup-context used for AggBuilder-routines.
func (e *EventsIO) Context() context.Context {
	return e.errGroupCtx
}

// Wait is a wrapper for errgroup.Wait, and will wait for all AggBuilder and RoutinesGroup
// routines to exit, and propagate the error from errgroup.Wait.
// This is not meant to do FanOut, it always returns the same channel.
// FanOut logic must be implemented by library-user.
// Note: The resulting channel will get data only once, and is then closed.
func (e *EventsIO) Wait() <-chan error {
	if e.waitChan == nil {
		e.waitChan = make(chan error)
		go func() {
			err := e.errGroup.Wait()
			e.waitChan <- err
			close(e.waitChan)
		}()
	}
	return (<-chan error)(e.waitChan)
}

// Close closes any open routines associated with AggBuilder service, such as
// Kafka Producers and Consumers. Use this when the service is no longer required.
func (e *EventsIO) Close() {
	e.ctxLock.Lock()
	if e.ctxOpen {
		e.ctxOpen = false
		e.closeChan <- struct{}{}
	}
	e.ctxLock.Unlock()
}

// BuildState gets events from EventStore-service.
func (e *EventsIO) BuildState(correlationID uuuid.UUID) (<-chan *EventResponse, error) {
	err := e.esReqProd.queryEventStore(correlationID)
	if err != nil {
		err = errors.Wrap(err, "Error creating ESQuery-Request")
		return nil, err
	}
	return (<-chan *EventResponse)(e.eventResp), nil
}
