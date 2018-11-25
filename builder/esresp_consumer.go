package builder

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/TerrexTech/uuuid"
	"github.com/pkg/errors"
)

// EventResponse is the response distributed over the EventsIO channels.
type EventResponse struct {
	Event model.Event
	Error error
}

type esRespHandlerConfig struct {
	aggID          int8
	metaCollection *mongo.Collection
	versionChan    chan int64
}

type collectConfig struct {
	enable bool

	eosToken         string
	eosCorrelationID uuuid.UUID
	timeoutCtx       context.Context

	respChan chan<- *EventResponse
}

// esRespHandler handler for Consumer Messages
type esRespHandler struct {
	*esRespHandlerConfig
	collect     collectConfig
	handlerLock sync.RWMutex
}

func newESRespHandler(config *esRespHandlerConfig) (*esRespHandler, error) {
	if config.aggID == 0 {
		return nil, errors.New("config error: aggID must be greater than 0")
	}
	if config.metaCollection == nil {
		return nil, errors.New("config error: metaCollection cannot be nil")
	}
	if config.versionChan == nil {
		return nil, errors.New("config error: versionChan cannot be nil")
	}

	return &esRespHandler{
		esRespHandlerConfig: config,
		collect:             collectConfig{},
	}, nil
}

func (e *esRespHandler) CollectEvents(
	eosToken string,
	eosCorrelationID uuuid.UUID,
	timeoutSec int,
) (<-chan *EventResponse, error) {
	e.handlerLock.RLock()
	collecting := e.collect.enable
	e.handlerLock.RUnlock()
	if collecting {
		return nil, errors.New("already collecting events")
	}

	ctx, _ := context.WithTimeout(
		context.Background(),
		time.Duration(timeoutSec)*time.Second,
	)
	respChan := make(chan *EventResponse)
	e.handlerLock.Lock()
	e.collect = collectConfig{
		enable:           true,
		eosToken:         eosToken,
		eosCorrelationID: eosCorrelationID,
		timeoutCtx:       ctx,
		respChan:         (chan<- *EventResponse)(respChan),
	}
	e.handlerLock.Unlock()

	go func() {
		<-ctx.Done()
		e.handlerLock.RLock()
		collecting := e.collect.enable
		e.handlerLock.RUnlock()

		if collecting {
			log.Println("ESResp-Consumer timed out")
			for {
				select {
				case esResp := <-respChan:
					if esResp != nil {
						log.Printf("Drained Event with UUID: %s", esResp.Event.UUID)
					}
				default:
					e.handlerLock.Lock()
					e.collect.enable = false
					close(respChan)
					e.handlerLock.Unlock()
				}
			}
		}
	}()

	return (<-chan *EventResponse)(respChan), nil
}

func (e *esRespHandler) Setup(sarama.ConsumerGroupSession) error {
	log.Println("Initializing ESQueryResponse-Consumer")
	go updateAggMeta(e.aggID, e.metaCollection, (<-chan int64)(e.versionChan))

	return nil
}

func (e *esRespHandler) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("Closing ESQueryRespConsumer")

	if &e.collect != nil {
		e.handlerLock.RLock()
		collectEnabled := e.collect.enable
		e.handlerLock.RUnlock()

		if collectEnabled {
			e.handlerLock.Lock()
			e.collect.enable = false
			close(e.collect.respChan)
			e.handlerLock.Unlock()
		}
	}
	return errors.New("ESQueryResponse-Consumer unexpectedly closed")
}

func (e *esRespHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	log.Println("ESQueryResponse-Consumer Listening...")
	for {
		select {
		case <-session.Context().Done():
			return errors.New("ESQueryResponse-Consumer: session closed")

		case msg := <-claim.Messages():
			if msg == nil {
				continue
			}

			doc := &model.Document{}
			err := json.Unmarshal(msg.Value, doc)
			if err != nil {
				session.MarkMessage(msg, "")
				err = errors.Wrap(err, "Error Unmarshalling ESQueryResponse into Document")
				log.Println(err)
				continue
			}
			log.Printf("Received ESQueryResponse with ID: %s", doc.UUID)

			var docError error
			if doc.Error != "" {
				docError = fmt.Errorf("Error %d: %s", doc.ErrorCode, doc.Error)
			}

			// Get all events from Document
			events := &[]model.Event{}
			err = json.Unmarshal(doc.Data, events)
			if err != nil {
				session.MarkMessage(msg, "")
				err = errors.Wrap(
					err,
					"ESQueryResponse-Consumer: Error Unmarshalling Document-response into Events",
				)
				log.Println(err)
				e.handlerLock.RLock()
				e.collect.respChan <- &EventResponse{
					Error: docError,
				}
				e.handlerLock.RUnlock()
			} else {
				e.handlerLock.RLock()
				collecting := e.collect.enable
				e.handlerLock.RUnlock()

				if collecting {
					// Distribute events to their respective channels
					for _, event := range *events {
						if event.UUID == (uuuid.UUID{}) {
							continue
						}

						e.handlerLock.RLock()
						eosToken := e.collect.eosToken
						correlationID := e.collect.eosCorrelationID
						e.handlerLock.RUnlock()

						if event.Action == eosToken && event.UUID == correlationID {
							log.Printf("Received EOS Event with UUID: %s %s", event.UUID, correlationID)
							e.handlerLock.Lock()
							e.collect.enable = false
							close(e.collect.respChan)
							e.handlerLock.Unlock()
							break
						}

						e.handlerLock.RLock()
						if e.collect.enable {
							e.collect.respChan <- &EventResponse{
								Event: event,
								Error: docError,
							}
						}
						e.handlerLock.RUnlock()
						e.versionChan <- event.Version
					}
				}
				session.MarkMessage(msg, "")

			}
		}
	}
}

func updateAggMeta(aggID int8, coll *mongo.Collection, versionChan <-chan int64) {
	for version := range versionChan {
		if version == 0 {
			continue
		}
		filter := &AggregateMeta{
			AggregateID: aggID,
		}
		update := map[string]int64{
			"version": version,
		}
		_, err := coll.UpdateMany(filter, update)
		// Non-fatal error, so we'll just print this to stdout for info, for now
		// This is Non-fatal because even if the Aggregate's version doesn't update, it'll only
		// mean that more events will have to be hydrated, which is more acceptable than
		// having the service exit.
		// TODO: Maybe define a limit on number of events?
		if err != nil {
			err = errors.Wrap(err, "Error updating AggregateMeta")
			log.Println(err)
		}
	}
	log.Println("--> Closed Aggregate meta-update routine")
}
