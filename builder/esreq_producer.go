package builder

import (
	"context"
	"encoding/json"
	"log"

	"github.com/TerrexTech/uuuid"

	"github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type esReqConfig struct {
	g        *errgroup.Group
	closeCtx context.Context

	aggID           int8
	mongoColl       *mongo.Collection
	kafkaProdConfig *kafka.ProducerConfig
	kafkaTopic      string
}

type esReqProducer struct {
	esReqConfig
	reqProducer *kafka.Producer
}

func newESReqProducer(config *esReqConfig) (*esReqProducer, error) {
	if config.g == nil {
		return nil, errors.New("errgroup cannot be nil")
	}
	if config.closeCtx == nil {
		return nil, errors.New("closeCtx cannot be nil")
	}
	if config.aggID <= 1 {
		return nil, errors.New("aggID cannot be less than 1")
	}
	if config.mongoColl == nil {
		return nil, errors.New("mongoColl cannot be nil")
	}
	if config.kafkaProdConfig == nil {
		return nil, errors.New("kafkaProdConfig cannot be nil")
	}
	if config.kafkaTopic == "" {
		return nil, errors.New("kafkaTopic cannot be blank")
	}

	p, err := kafka.NewProducer(config.kafkaProdConfig)
	if err != nil {
		err = errors.Wrap(err, "Error creating ESQueryRequest-Producer")
		return nil, err
	}

	config.g.Go(func() error {
		var prodErr error
	errLoop:
		for {
			select {
			case <-config.closeCtx.Done():
				log.Println("ESQueryRequest-Producer: session closed")
				prodErr = p.Close()
				break errLoop
			case err := <-p.Errors():
				if err != nil && err.Err != nil {
					parsedErr := errors.Wrap(err.Err, "Error in ESQueryRequest-Producer")
					log.Println(parsedErr)
					log.Println(err)
				}
			}
		}
		log.Println("--> Closed ESQueryRequest-Producer error-routine")
		return prodErr
	})
	return &esReqProducer{
		esReqConfig: *config,
		reqProducer: p,
	}, nil
}

// newESReqProducer produces the EventStoreQuery requests to get new events.
// This is triggered everytime an event is produced.
func (er *esReqProducer) queryEventStore(correlationID uuuid.UUID) error {
	currVersion, err := getVersion(er.aggID, er.mongoColl)
	if err != nil {
		err = errors.Wrapf(
			err,
			"Error fetching max version for AggregateID %d",
			er.aggID,
		)
		log.Println(err)
		return err
	}

	cid, err := uuuid.NewV4()
	if err != nil {
		err = errors.Wrap(err, "Error generating UUID for ESQuery")
		log.Println(err)
		cid = uuuid.UUID{}
	}
	// Create EventStoreQuery
	esQuery := model.EventStoreQuery{
		AggregateID:      er.aggID,
		AggregateVersion: currVersion,
		CorrelationID:    cid,
		// Hardcoding for now, otherwise we'll calculate this based
		// on current year, and account for NewYear-buffers and such
		YearBucket: 2018,
		UUID:       correlationID,
	}
	esMsg, err := json.Marshal(esQuery)
	if err != nil {
		err = errors.Wrap(err, "ESQueryRequest-Producer: Error Marshalling EventStoreQuery")
		log.Println(err)
		return err
	}
	msg := kafka.CreateMessage(er.kafkaTopic, esMsg)
	er.reqProducer.Input() <- msg
	return nil
}
