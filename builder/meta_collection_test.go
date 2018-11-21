package builder

import (
	"context"
	"math/rand"
	"os"
	"time"

	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-mongoutils/mongo"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("MetaCollection", func() {
	var (
		aggID    int8
		conn     *mongo.ConnectionConfig
		database string
	)

	BeforeEach(func() {
		database = os.Getenv("MONGO_DATABASE")

		mongoHosts := commonutil.ParseHosts(
			os.Getenv("MONGO_HOSTS"),
		)
		mongoUsername := os.Getenv("MONGO_USERNAME")
		mongoPassword := os.Getenv("MONGO_PASSWORD")

		config := mongo.ClientConfig{
			Hosts:               *mongoHosts,
			Username:            mongoUsername,
			Password:            mongoPassword,
			TimeoutMilliseconds: 5000,
		}

		client, err := mongo.NewClient(config)
		Expect(err).ToNot(HaveOccurred())

		conn = &mongo.ConnectionConfig{
			Client:  client,
			Timeout: 5000,
		}

		dbCtx, dbCancel := context.WithTimeout(context.Background(), time.Second*5000)
		err = client.Database(database).Drop(dbCtx)
		dbCancel()
		Expect(err).ToNot(HaveOccurred())

		// Generate random AggregateID
		minID := 10
		maxID := 100
		s1 := rand.NewSource(time.Now().UnixNano())
		r1 := rand.New(s1)
		aggID = int8(r1.Intn(maxID-minID) + minID)
	})

	It("should create Meta-Collection with Aggregate-meta", func() {
		// Choose a random AggregateID and see if the record is created in meta-table
		coll, err := createMetaCollection(aggID, conn, database, "test_meta")
		Expect(err).ToNot(HaveOccurred())

		result, err := coll.FindOne(&AggregateMeta{
			AggregateID: aggID,
		})
		Expect(err).ToNot(HaveOccurred())
		meta, assertOK := result.(*AggregateMeta)
		Expect(assertOK).To(BeTrue())
		Expect(meta.AggregateID).To(Equal(aggID))
		Expect(meta.Version).To(Equal(int64(1)))
	})

	It("should get correct AggregateVersion", func() {
		// Insert a meta-record and see if version is fetched correctly
		coll, err := createMetaCollection(aggID, conn, database, "test_meta")
		Expect(err).ToNot(HaveOccurred())

		_, err = coll.InsertOne(&AggregateMeta{
			AggregateID: 101,
			Version:     42,
		})
		Expect(err).ToNot(HaveOccurred())

		result, err := coll.FindOne(&AggregateMeta{
			AggregateID: 101,
		})
		Expect(err).ToNot(HaveOccurred())
		meta, assertOK := result.(*AggregateMeta)
		Expect(assertOK).To(BeTrue())
		Expect(meta.AggregateID).To(Equal(int8(101)))
		Expect(meta.Version).To(Equal(int64(42)))
	})
})
