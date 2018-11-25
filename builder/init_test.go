package builder

import (
	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/TerrexTech/go-mongoutils/mongo"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("InitTest", func() {
	var ioConfig IOConfig

	BeforeEach(func() {
		kc := KafkaConfig{
			ESQueryResCons: &kafka.ConsumerConfig{},
			ESQueryReqProd: &kafka.ProducerConfig{},

			ESQueryReqTopic: "testpeqt",
		}
		mc := MongoConfig{
			AggCollection:      &mongo.Collection{},
			AggregateID:        2,
			Connection:         &mongo.ConnectionConfig{},
			MetaDatabaseName:   "test_db",
			MetaCollectionName: "test_coll",
		}
		ioConfig = IOConfig{
			KafkaConfig: kc,
			MongoConfig: mc,
		}
	})

	Describe("MongoConfig Validation", func() {
		It("should return error if AggregateID is not specified", func() {
			ioConfig.MongoConfig.AggregateID = 0
			eventsIO, err := Init(ioConfig)
			Expect(err).To(HaveOccurred())
			Expect(eventsIO).To(BeNil())

			ioConfig.MongoConfig.AggregateID = -2
			eventsIO, err = Init(ioConfig)
			Expect(err).To(HaveOccurred())
			Expect(eventsIO).To(BeNil())
		})

		It("should return error if AggCollection is not specified", func() {
			ioConfig.MongoConfig.AggCollection = nil
			eventsIO, err := Init(ioConfig)
			Expect(err).To(HaveOccurred())
			Expect(eventsIO).To(BeNil())
		})

		It("should return error if Connection is not specified", func() {
			ioConfig.MongoConfig.Connection = nil
			eventsIO, err := Init(ioConfig)
			Expect(err).To(HaveOccurred())
			Expect(eventsIO).To(BeNil())
		})

		It("should return error if MetaDatabaseName is not specified", func() {
			ioConfig.MongoConfig.MetaDatabaseName = ""
			eventsIO, err := Init(ioConfig)
			Expect(err).To(HaveOccurred())
			Expect(eventsIO).To(BeNil())
		})
	})

	Describe("KafkaConfig Validation", func() {
		It("should return error if ESQueryResCons is not specified", func() {
			ioConfig.KafkaConfig.ESQueryResCons = nil
			eventsIO, err := Init(ioConfig)
			Expect(err).To(HaveOccurred())
			Expect(eventsIO).To(BeNil())
		})

		It("should return error if EOSToken is not specified", func() {
			ioConfig.KafkaConfig.EOSToken = ""
			eventsIO, err := Init(ioConfig)
			Expect(err).To(HaveOccurred())
			Expect(eventsIO).To(BeNil())
		})

		It("should return error if ESQueryReqProd is not specified", func() {
			ioConfig.KafkaConfig.ESQueryReqProd = nil
			eventsIO, err := Init(ioConfig)
			Expect(err).To(HaveOccurred())
			Expect(eventsIO).To(BeNil())
		})

		It("should return error if ESQueryReqTopic is not specified", func() {
			ioConfig.KafkaConfig.ESQueryReqTopic = ""
			eventsIO, err := Init(ioConfig)
			Expect(err).To(HaveOccurred())
			Expect(eventsIO).To(BeNil())
		})
	})
})
