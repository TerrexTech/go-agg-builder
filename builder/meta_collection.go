package builder

import (
	"log"

	"github.com/mongodb/mongo-go-driver/bson/objectid"

	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/pkg/errors"
)

// AggregateMeta contains current versions for Aggregates.
// This version is updated everytime an Aggregate receives an associated Event.
type AggregateMeta struct {
	ID          objectid.ObjectID `bson:"_id,omitempty" json:"_id,omitempty"`
	AggregateID int8              `bson:"aggregateID,omitempty" json:"aggregateID,omitempty"`
	Version     int64             `bson:"version,omitempty" json:"version,omitempty"`
}

// createMetaCollection ensures that AggregateMeta collection exists,
// and adds meta-entry to it if required.
func createMetaCollection(
	aggID int8,
	conn *mongo.ConnectionConfig,
	db string,
	coll string,
) (*mongo.Collection, error) {
	// Index Configuration
	indexConfigs := []mongo.IndexConfig{
		mongo.IndexConfig{
			ColumnConfig: []mongo.IndexColumnConfig{
				mongo.IndexColumnConfig{
					Name: "aggregateID",
				},
			},
			IsUnique: true,
			Name:     "aggregateID_index",
		},
	}

	c := &mongo.Collection{
		Connection:   conn,
		Name:         coll,
		Database:     db,
		SchemaStruct: &AggregateMeta{},
		Indexes:      indexConfigs,
	}
	collection, err := mongo.EnsureCollection(c)
	if err != nil {
		err = errors.Wrap(err, "CreateMetaCollection: Error in EnsureCollection")
		return nil, err
	}

	err = ensureAggMetaEntry(aggID, collection)
	if err != nil {
		err = errors.Wrap(err, "CreateMetaCollection: Error in ensureAggMetaEntry")
		return nil, err
	}
	return collection, nil
}

// ensureAggMetaEntry adds meta-entry to AggregateMeta collection if required.
func ensureAggMetaEntry(aggID int8, collection *mongo.Collection) error {
	_, err := collection.FindOne(&AggregateMeta{
		AggregateID: aggID,
	})
	if err == nil {
		return nil
	}

	err = errors.Wrap(err, "EnsureAggMetaEntry: Error while getting meta-entry")
	log.Println(err)
	log.Println("New entry will be created")

	_, err = collection.InsertOne(&AggregateMeta{
		AggregateID: aggID,
		Version:     1,
	})
	if err != nil {
		err = errors.Wrap(err, "EnsureAggMetaEntry: Error while creating meta-entry")
		return err
	}
	return nil
}

func getVersion(aggID int8, c *mongo.Collection) (int64, error) {
	result, err := c.FindOne(&AggregateMeta{
		AggregateID: aggID,
	})
	if err != nil {
		err = errors.Wrap(err, "GetVersion: Error getting AggregateMeta")
		return -1, err
	}

	meta, assertOK := result.(*AggregateMeta)
	if !assertOK {
		err = errors.New(
			"GetVersion: Error asserting result into AggregateMeta",
		)
		log.Printf("%+v", result)
		return -1, err
	}
	return meta.Version, nil
}
