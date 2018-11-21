package builder

import (
	"encoding/json"
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/TerrexTech/uuuid"
	"github.com/joho/godotenv"
	"github.com/mongodb/mongo-go-driver/bson/objectid"
	"github.com/pkg/errors"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestBuilder(t *testing.T) {
	log.Println("Reading environment file")
	err := godotenv.Load("../test.env")
	if err != nil {
		err = errors.Wrap(err,
			".env file not found, env-vars will be read as set in environment",
		)
		log.Println(err)
	}

	missingVar, err := commonutil.ValidateEnv(
		"KAFKA_BROKERS",

		"KAFKA_EVENT_TOPIC",
		"KAFKA_CMD_TOPIC",
		"KAFKA_CONSUMER_EVENT_QUERY_GROUP",
		"KAFKA_CONSUMER_EVENT_QUERY_TOPIC",

		"KAFKA_CMD_TOPIC",
		"KAFKA_PRODUCER_EVENT_QUERY_TOPIC",

		"MONGO_HOSTS",
		"MONGO_USERNAME",
		"MONGO_PASSWORD",
		"MONGO_DATABASE",
		"MONGO_CONNECTION_TIMEOUT_MS",
		"MONGO_RESOURCE_TIMEOUT_MS",
	)

	if err != nil {
		err = errors.Wrapf(
			err,
			"Env-var %s is required for testing, but is not set", missingVar,
		)
		log.Fatalln(err)
	}

	RegisterFailHandler(Fail)
	RunSpecs(t, "TestBuilder Suite")
}

type item struct {
	ID         objectid.ObjectID `bson:"_id,omitempty" json:"_id,omitempty"`
	Word       string            `bson:"word,omitempty" json:"word,omitempty"`
	Definition string            `bson:"definition,omitempty" json:"definition,omitempty"`
	Hits       int               `bson:"hits,omitempty" json:"hits,omitempty"`
}

func mockEvent(
	input chan<- *sarama.ProducerMessage,
	topic string,
	aggID int8,
) *model.Event {
	eventUUID, err := uuuid.NewV4()
	Expect(err).ToNot(HaveOccurred())
	cid, err := uuuid.NewV4()
	Expect(err).ToNot(HaveOccurred())

	mockEvent := &model.Event{
		AggregateID:   aggID,
		Action:        "test-action",
		CorrelationID: cid,
		Data:          []byte("test-data"),
		Source:        "test-source",
		NanoTime:      time.Now().UnixNano(),
		UUID:          eventUUID,
		YearBucket:    2018,
	}

	// Produce command on Kafka topic
	testEventMsg, err := json.Marshal(mockEvent)
	Expect(err).ToNot(HaveOccurred())

	input <- kafka.CreateMessage(topic, testEventMsg)
	log.Printf("====> Produced mock event: %s on topic: %s", eventUUID, topic)
	return mockEvent
}

func randomAggID() int8 {
	// Generate a random AggregateID.
	// TODO: Find better way to test using fresh aggregates
	// (delete and recreate test-database?)
	minID := 10
	maxID := 100
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	aggID := int8(r1.Intn(maxID-minID) + minID)
	return aggID
}
