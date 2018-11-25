package builder

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/TerrexTech/go-common-models/model"

	"github.com/TerrexTech/uuuid"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/TerrexTech/go-mongoutils/mongo"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
)

var _ = Describe("Consumers", func() {
	var (
		kafkaBrokers []string
		metaColl     *mongo.Collection

		cmdTopic       string
		eventsTopic    string
		eventProdInput chan<- *sarama.ProducerMessage

		ioConfig IOConfig
		aggID    int8
	)

	BeforeSuite(func() {
		aggID = randomAggID()

		// ==========> Mongo Setup
		mongoHosts := commonutil.ParseHosts(
			os.Getenv("MONGO_HOSTS"),
		)
		mongoUsername := os.Getenv("MONGO_USERNAME")
		mongoPassword := os.Getenv("MONGO_PASSWORD")

		mongoConnTimeoutStr := os.Getenv("MONGO_CONNECTION_TIMEOUT_MS")
		mongoConnTimeout, err := strconv.Atoi(mongoConnTimeoutStr)
		if err != nil {
			err = errors.Wrap(err, "Error converting MONGO_CONNECTION_TIMEOUT_MS to integer")
			log.Println(err)
			log.Println("A defalt value of 3000 will be used for MONGO_CONNECTION_TIMEOUT_MS")
			mongoConnTimeout = 3000
		}

		mongoResTimeoutStr := os.Getenv("MONGO_CONNECTION_TIMEOUT_MS")
		mongoResTimeout, err := strconv.Atoi(mongoResTimeoutStr)
		if err != nil {
			err = errors.Wrap(err, "Error converting MONGO_RESOURCE_TIMEOUT_MS to integer")
			log.Println(err)
			log.Println("A defalt value of 5000 will be used for MONGO_RESOURCE_TIMEOUT_MS")
			mongoConnTimeout = 5000
		}

		mongoDatabase := os.Getenv("MONGO_DATABASE")

		mongoConfig := mongo.ClientConfig{
			Hosts:               *mongoHosts,
			Username:            mongoUsername,
			Password:            mongoPassword,
			TimeoutMilliseconds: uint32(mongoConnTimeout),
		}

		// ====> MongoDB Client
		client, err := mongo.NewClient(mongoConfig)
		Expect(err).ToNot(HaveOccurred())

		mongoConn := &mongo.ConnectionConfig{
			Client:  client,
			Timeout: uint32(mongoResTimeout),
		}
		metaColl, err = createMetaCollection(aggID, mongoConn, mongoDatabase, "test_meta")
		Expect(err).ToNot(HaveOccurred())

		// Index Configuration
		indexConfigs := []mongo.IndexConfig{
			mongo.IndexConfig{
				ColumnConfig: []mongo.IndexColumnConfig{
					mongo.IndexColumnConfig{
						Name:        "word",
						IsDescOrder: true,
					},
				},
				IsUnique: true,
				Name:     "test_index",
			},
		}

		// ====> Create New Collection
		c := &mongo.Collection{
			Connection:   mongoConn,
			Name:         "test_coll",
			Database:     mongoDatabase,
			SchemaStruct: &item{},
			Indexes:      indexConfigs,
		}
		collection, err := mongo.EnsureCollection(c)
		Expect(err).ToNot(HaveOccurred())

		// ==========> Kafka Setup
		kafkaBrokers = *commonutil.ParseHosts(
			os.Getenv("KAFKA_BROKERS"),
		)
		cmdTopic = fmt.Sprintf(
			"%s.%d",
			os.Getenv("KAFKA_CMD_TOPIC"), aggID,
		)
		eventsTopic = os.Getenv("KAFKA_EVENT_TOPIC")

		cESQueryGroup := os.Getenv("KAFKA_CONSUMER_EVENT_QUERY_GROUP")
		pESQueryTopic := os.Getenv("KAFKA_PRODUCER_EVENT_QUERY_TOPIC")

		cESQueryTopic := fmt.Sprintf(
			"%s.%d",
			os.Getenv("KAFKA_CONSUMER_EVENT_QUERY_TOPIC"), aggID,
		)
		eosToken := os.Getenv("KAFKA_END_OF_STREAM_TOKEN")

		kc := KafkaConfig{
			ESQueryResCons: &kafka.ConsumerConfig{
				KafkaBrokers: kafkaBrokers,
				GroupName:    cESQueryGroup,
				Topics:       []string{cESQueryTopic},
			},
			EOSToken: eosToken,

			ESQueryReqProd: &kafka.ProducerConfig{
				KafkaBrokers: kafkaBrokers,
			},
			ESQueryReqTopic: pESQueryTopic,
		}
		mc := MongoConfig{
			AggregateID:        aggID,
			AggCollection:      collection,
			Connection:         mongoConn,
			MetaDatabaseName:   mongoDatabase,
			MetaCollectionName: "test_meta",
		}
		ioConfig = IOConfig{
			KafkaConfig: kc,
			MongoConfig: mc,
		}

		prodConfig := &kafka.ProducerConfig{
			KafkaBrokers: kafkaBrokers,
		}
		log.Println("Creating Kafka mock-event Producer")
		p, err := kafka.NewProducer(prodConfig)
		Expect(err).ToNot(HaveOccurred())
		eventProdInput = p.Input()
	})

	Context("Events are fetched", func() {
		Specify("events should appear on event channel", func() {
			mockEvent := mockEvent(eventProdInput, eventsTopic, aggID)

			log.Println(cmdTopic)
			log.Println(
				"Checking if the event-channels received the event, " +
					"with timeout of 20 seconds",
			)

			eventsIO, err := Init(ioConfig)
			Expect(err).ToNot(HaveOccurred())

			eventSuccess := false
			var eventLock sync.RWMutex

			g := eventsIO.ErrGroup()
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()

			time.Sleep(5 * time.Second)
			g.Go(func() error {
				defer GinkgoRecover()

				events, err := eventsIO.BuildState(mockEvent.UUID, 15)
				Expect(err).ToNot(HaveOccurred())
				// Allow time for event to be processed by event-persistence and such
				time.Sleep(5 * time.Second)
				for {
					select {
					case <-ctx.Done():
						return errors.New("timed out")
					case eventResp := <-events:
						if eventResp == nil {
							continue
						}
						e := eventResp.Event
						Expect(eventResp.Error).ToNot(HaveOccurred())

						log.Println("An Event appeared on event channel")
						cidMatch := e.CorrelationID == mockEvent.CorrelationID
						uuidMatch := e.UUID == mockEvent.UUID
						if uuidMatch && cidMatch {
							log.Println("==> A matching Event appeared on event channel")
							eventLock.Lock()
							eventSuccess = true
							eventLock.Unlock()
							return nil
						}
					}
				}
			})

		resultTimeoutLoop:
			for {
				select {
				case <-ctx.Done():
					break resultTimeoutLoop
				default:
					eventLock.RLock()
					es := eventSuccess
					eventLock.RUnlock()
					if es {
						break resultTimeoutLoop
					}
				}
			}

			eventsIO.Close()
			<-eventsIO.Wait()
			Expect(eventSuccess).To(BeTrue())
		})

		Context("EOSEvent is not received within specified Timeout", func() {
			var eventID uuuid.UUID

			BeforeEach(func() {
				// Produce mock-EventStoreQuery response that doesn't send EOSEvent
				kc := ioConfig.KafkaConfig

				prod, err := kafka.NewProducer(kc.ESQueryReqProd)
				Expect(err).ToNot(HaveOccurred())

				eventID, err = uuuid.NewV4()
				Expect(err).ToNot(HaveOccurred())
				mockEvent := []model.Event{
					model.Event{
						Action: "test-action",
						UUID:   eventID,
					},
				}
				marshalEvent, err := json.Marshal(mockEvent)
				Expect(err).ToNot(HaveOccurred())

				docID, err := uuuid.NewV4()
				Expect(err).ToNot(HaveOccurred())
				doc := model.Document{
					Data: marshalEvent,
					UUID: docID,
				}
				marshalDoc, err := json.Marshal(doc)
				Expect(err).ToNot(HaveOccurred())
				mockESResp := kafka.CreateMessage(kc.ESQueryResCons.Topics[0], marshalDoc)
				prod.Input() <- mockESResp
				err = prod.Close()
				Expect(err).ToNot(HaveOccurred())
			})

			It("should close response-channel on timeout", func(done Done) {
				// Check if timeout-mechanism closes the channel so the code can proceed
				esRespHandler, err := newESRespHandler(&esRespHandlerConfig{
					aggID:          ioConfig.MongoConfig.AggregateID,
					metaCollection: metaColl,
					versionChan:    make(chan int64, 128),
				})
				Expect(err).ToNot(HaveOccurred())
				esRespConsumer, err := kafka.NewConsumer(ioConfig.KafkaConfig.ESQueryResCons)
				Expect(err).ToNot(HaveOccurred())
				go func() {
					defer GinkgoRecover()
					err := esRespConsumer.Consume(context.Background(), esRespHandler)
					Expect(err).To(HaveOccurred())
				}()

				defer func() {
					defer GinkgoRecover()
					err := esRespConsumer.Close()
					Expect(err).ToNot(HaveOccurred())
				}()

				cid, err := uuuid.NewV4()
				Expect(err).ToNot(HaveOccurred())
				eventResp, err := esRespHandler.CollectEvents(ioConfig.KafkaConfig.EOSToken, cid, 15)
				Expect(err).ToNot(HaveOccurred())
				eventReceived := false
				for er := range eventResp {
					if er.Event.UUID == eventID {
						Expect(er.Error).ToNot(HaveOccurred())
						eventReceived = true
					}
				}

				close(done)
				Expect(eventReceived).To(BeTrue())
			}, 20)
		})
	})

	It("should update Aggregate-meta", func() {
		aggID := randomAggID()

		// Generate a mock-Event
		mockEvent(eventProdInput, eventsTopic, aggID)

		cid, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		ioConfig.MongoConfig.AggregateID = aggID
		eventsIO, err := Init(ioConfig)
		Expect(err).ToNot(HaveOccurred())
		_, err = eventsIO.BuildState(cid, 15)
		Expect(err).ToNot(HaveOccurred())

		// Allow time for event to be processed by event-persistence and such
		time.Sleep(5 * time.Second)

		mc := ioConfig.MongoConfig
		c := &mongo.Collection{
			Connection:   mc.Connection,
			Name:         mc.MetaCollectionName,
			Database:     mc.MetaDatabaseName,
			SchemaStruct: &AggregateMeta{},
		}

		collection, err := mongo.EnsureCollection(c)
		Expect(err).ToNot(HaveOccurred())
		result, err := collection.FindOne(&AggregateMeta{
			AggregateID: aggID,
		})
		Expect(err).ToNot(HaveOccurred())
		meta, assertOK := result.(*AggregateMeta)
		Expect(assertOK).To(BeTrue())
		Expect(meta.AggregateID).To(Equal(aggID))
		Expect(meta.Version > 0).To(BeTrue())
	})

	It("should execute cancel-context when the service is closed", func(done Done) {
		kc := ioConfig.KafkaConfig
		// Change ConsumerGroup so it doesn't interfere with other tests' groups
		kc.ESQueryResCons.GroupName = "test-eq-group-close-2"

		eventsIO, err := Init(ioConfig)
		Expect(err).ToNot(HaveOccurred())

		eventsIO.Close()
		<-eventsIO.Wait()
		close(done)
	}, 15)
})
