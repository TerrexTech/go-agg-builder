package builder

import (
	"context"
	"sync"

	"github.com/TerrexTech/uuuid"
	"github.com/pkg/errors"

	"golang.org/x/sync/errgroup"
)

type ctxConfig struct {
	ErrGroupCtx context.Context
	ErrGroup    *errgroup.Group
	CloseChan   chan<- struct{}

	ctxOpen  bool
	ctxLock  sync.RWMutex
	waitChan chan error
}

type esConfig struct {
	ESReqProd *esReqProducer

	ESRespHandler *esRespHandler
	EOSToken      string
}

// EventsIO allows interacting with the AggBuilder service.
// This is the medium through which the new events are distributed.
type EventsIO struct {
	ctx      *ctxConfig
	esConfig *esConfig
}

func newEventsIO(ctx *ctxConfig, esConfig *esConfig) (*EventsIO, error) {
	if ctx.CloseChan == nil {
		return nil, errors.New("ctxConfig: CloseChan cannot be nil")
	}
	if ctx.ErrGroup == nil {
		return nil, errors.New("ctxConfig: ErrGroup cannot be nil")
	}
	if ctx.ErrGroupCtx == nil {
		return nil, errors.New("ctxConfig: ErrGroupCtx cannot be nil")
	}

	if esConfig.ESReqProd == nil {
		return nil, errors.New("esConfig: ESReqProd cannot be nil")
	}
	if esConfig.ESRespHandler == nil {
		return nil, errors.New("esConfig: ESRespHandler cannot be nil")
	}
	if esConfig.EOSToken == "" {
		return nil, errors.New("esConfig: EOSToken cannot be blank")
	}

	return &EventsIO{
		ctx: &ctxConfig{
			CloseChan:   ctx.CloseChan,
			ErrGroup:    ctx.ErrGroup,
			ErrGroupCtx: ctx.ErrGroupCtx,

			ctxOpen: true,
			ctxLock: sync.RWMutex{},
		},
		esConfig: esConfig,
	}, nil
}

// ErrGroup returns the errgroup used for AggBuilder-routines.
func (e *EventsIO) ErrGroup() *errgroup.Group {
	return e.ctx.ErrGroup
}

// Context returns the errgroup-context used for AggBuilder-routines.
func (e *EventsIO) Context() context.Context {
	return e.ctx.ErrGroupCtx
}

// Wait is a wrapper for errgroup.Wait, and will wait for all AggBuilder and RoutinesGroup
// routines to exit, and propagate the error from errgroup.Wait.
// This is not meant to do FanOut, it always returns the same channel.
// FanOut logic must be implemented by library-user.
// Note: The resulting channel will get data only once, and is then closed.
func (e *EventsIO) Wait() <-chan error {
	if e.ctx.waitChan == nil {
		e.ctx.waitChan = make(chan error)
		go func() {
			err := e.ctx.ErrGroup.Wait()
			e.ctx.waitChan <- err
			close(e.ctx.waitChan)
		}()
	}
	return (<-chan error)(e.ctx.waitChan)
}

// Close closes any open routines associated with AggBuilder service, such as
// Kafka Producers and Consumers. Use this when the service is no longer required.
func (e *EventsIO) Close() {
	e.ctx.ctxLock.Lock()
	if e.ctx.ctxOpen {
		e.ctx.ctxOpen = false
		e.ctx.CloseChan <- struct{}{}
	}
	e.ctx.ctxLock.Unlock()
}

// BuildState gets events from EventStore-service.
func (e *EventsIO) BuildState(
	correlationID uuuid.UUID,
	timeoutSec int,
) (<-chan *EventResponse, error) {
	es := e.esConfig

	err := es.ESReqProd.queryEventStore(correlationID)
	if err != nil {
		err = errors.Wrap(err, "Error creating ESQuery-Request")
		return nil, err
	}

	respChan, err := es.ESRespHandler.CollectEvents(
		e.esConfig.EOSToken,
		correlationID,
		timeoutSec,
	)
	if err != nil {
		err = errors.Wrap(err, "Error getting EventResp channel")
		return nil, err
	}
	return respChan, nil
}
