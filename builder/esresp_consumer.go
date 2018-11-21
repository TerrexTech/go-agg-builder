package builder

import (
	"encoding/json"
	"fmt"
	"log"

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

// esRespHandler handler for Consumer Messages
type esRespHandler struct {
	aggID          int8
	eventResp      chan<- *EventResponse
	metaCollection *mongo.Collection

	versionChan chan int64
}

func (e *esRespHandler) Setup(sarama.ConsumerGroupSession) error {
	log.Println("Initializing ESQueryResponse-Consumer")
	go updateAggMeta(e.aggID, e.metaCollection, (<-chan int64)(e.versionChan))

	return nil
}

func (e *esRespHandler) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("Closing ESQueryRespConsumer")

	close(e.eventResp)
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
				err = errors.Wrap(
					err,
					"ESQueryResponse-Consumer: Error Unmarshalling Document-response into Events",
				)
				log.Println(err)
				continue
			}

			// Distribute events to their respective channels
			for _, event := range *events {
				if event.UUID == (uuuid.UUID{}) {
					continue
				}
				e.eventResp <- &EventResponse{
					Event: event,
					Error: docError,
				}
				e.versionChan <- event.Version
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
