package builder

import (
	"context"
	"log"

	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// KafkaConfig is the configuration for Kafka, such as brokers and topics.
type KafkaConfig struct {
	// Consumer for EventStoreQuery-response
	// The first topic in Consumer-Topics is used as "Topic" field for EventStoreQuery.
	ESQueryResCons *kafka.ConsumerConfig
	EOSToken       string

	// Producer for making requests to ESQuery
	ESQueryReqProd *kafka.ProducerConfig
	// Topic on which requests to EventStoreQuery should be sent.
	ESQueryReqTopic string
}

// MongoConfig is the configuration for MongoDB client.
type MongoConfig struct {
	AggregateID   int8
	AggCollection *mongo.Collection
	// Collection/Database in which Aggregate metadata is stored.
	Connection         *mongo.ConnectionConfig
	MetaDatabaseName   string
	MetaCollectionName string
}

// IOConfig is the configuration for AggBuilder service.
type IOConfig struct {
	KafkaConfig KafkaConfig
	MongoConfig MongoConfig
}

// validateConfig validates the input config.
func validateConfig(config IOConfig) error {
	mc := config.MongoConfig
	if mc.AggregateID < 1 {
		return errors.New(
			"MongoConfig: AggregateID >0 is required, but none/invalid was specified",
		)
	}
	if mc.AggCollection == nil {
		return errors.New(
			"MongoConfig: AggCollection is required, but none was specified",
		)
	}
	if mc.Connection == nil {
		return errors.New(
			"MongoConfig: Connection is required, but none was specified",
		)
	}
	if mc.MetaDatabaseName == "" {
		return errors.New(
			"MongoConfig: MetaDatabaseName is required, but none was specified",
		)
	}
	if mc.MetaCollectionName == "" {
		return errors.New(
			"MongoConfig: MetaCollectionName is required, but none was specified",
		)
	}

	kc := config.KafkaConfig
	if kc.ESQueryResCons == nil {
		return errors.New(
			"KafkaConfig: ESQueryResCons is required, but none was specified",
		)
	}
	if kc.EOSToken == "" {
		return errors.New(
			"KafkaConfig: EOSToken is required, but none was specified",
		)
	}
	if kc.ESQueryReqProd == nil {
		return errors.New(
			"KafkaConfig: ESQueryReqProd is required, but none was specified",
		)
	}
	if kc.ESQueryReqTopic == "" {
		return errors.New(
			"KafkaConfig: ESQueryReqTopic is required, but none was specified",
		)
	}

	return nil
}

// Init initializes the AggBuilder service.
func Init(config IOConfig) (*EventsIO, error) {
	log.Println("Initializing Builder Service")

	err := validateConfig(config)
	if err != nil {
		err = errors.Wrap(err, "Error validating Configuration")
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)

	// closeChan will close all components of AggBuilder when anything is sent to it.
	closeChan := make(chan struct{})

	kfConfig := config.KafkaConfig
	mgConfig := config.MongoConfig

	metaCollection, err := createMetaCollection(
		mgConfig.AggregateID,
		mgConfig.Connection,
		mgConfig.MetaDatabaseName,
		mgConfig.MetaCollectionName,
	)
	if err != nil {
		err = errors.Wrap(err, "Error initializing Meta-Collection")
		cancel()
		return nil, err
	}

	// ESQueryRequest-Producer
	log.Println("Initializing ESQueryRequest-Producer")
	esReqProd, err := newESReqProducer(&esReqConfig{
		g:        g,
		closeCtx: ctx,

		aggID:           mgConfig.AggregateID,
		kafkaProdConfig: kfConfig.ESQueryReqProd,
		kafkaTopic:      kfConfig.ESQueryReqTopic,
		respTopic:       kfConfig.ESQueryResCons.Topics[0],
		mongoColl:       metaCollection,
	})
	if err != nil {
		err = errors.Wrap(err, "Error creating ESQueryRequest-Producer")
		cancel()
		return nil, err
	}

	// ESQueryResponse-Consumer
	log.Println("Initializing ESQueryResponse-Consumer")
	esRespConsumer, err := kafka.NewConsumer(kfConfig.ESQueryResCons)
	if err != nil {
		err = errors.Wrap(err, "Error creating ESQueryResponse-Consumer")
		cancel()
		return nil, err
	}
	g.Go(func() error {
		var consErr error
	errLoop:
		for {
			select {
			case <-ctx.Done():
				break errLoop
			case err := <-esRespConsumer.Errors():
				if err != nil {
					err = errors.Wrap(err, "Error in ESQueryResponse-Consumer")
					log.Println(err)
					consErr = err
					break errLoop
				}
			}
		}
		log.Println("--> Closed ESQueryResponse-Consumer error-routine")
		return consErr
	})

	log.Println("Starting ESResponse Consumer")
	esRespHandler, err := newESRespHandler(&esRespHandlerConfig{
		aggID:          config.MongoConfig.AggregateID,
		metaCollection: metaCollection,
		versionChan:    make(chan int64, 128),
	})
	if err != nil {
		err = errors.Wrap(err, "Error initializing ESResponse Consumer")
		cancel()
		return nil, err
	}

	// ESQueryResponse-Consumer Messages
	g.Go(func() error {
		err = esRespConsumer.Consume(ctx, esRespHandler)
		if err != nil {
			err = errors.Wrap(err, "Failed to consume ESQueryResponse")
		}
		log.Println("--> Closed ESQueryResponse-Consumer")
		return err
	})

	eventsIOCtx := &ctxConfig{
		ErrGroupCtx: ctx,
		ErrGroup:    g,
		CloseChan:   closeChan,
	}
	eventsIOESConfig := &esConfig{
		ESReqProd:     esReqProd,
		ESRespHandler: esRespHandler,
		EOSToken:      kfConfig.EOSToken,
	}
	eventsIO, err := newEventsIO(eventsIOCtx, eventsIOESConfig)
	if err != nil {
		err = errors.Wrap(err, "Error initializing EventsIO")
		cancel()
		return nil, err
	}
	g.Go(func() error {
		<-closeChan
		eventsIO.ctx.ctxLock.Lock()
		eventsIO.ctx.ctxOpen = false
		eventsIO.ctx.ctxLock.Unlock()

		log.Println("Received Close signal")
		log.Println("Signalling routines to close")
		cancel()
		close(closeChan)
		return nil
	})
	g.Go(func() error {
		<-ctx.Done()
		eventsIO.Close()
		return nil
	})

	log.Println("Aggregate-Builder Service Initialized")
	return eventsIO, nil
}
