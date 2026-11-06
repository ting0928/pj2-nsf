package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

/*
customDb is a MongoDB implementation of DbIf.

It is selected with `db_type: "mongodb"` in config.yaml. The TA's environment never uses it,
so the connection settings are hard-coded here as the assignment allows.

Every key/value pair is stored as one document: {_id: <key>, value: <value>}.
*/

const (
	mongoUri        = "mongodb://127.0.0.1:27018"
	mongoDatabase   = "nsf"
	mongoCollection = "services"
	mongoTimeout    = 5 * time.Second
)

type mongoDocument struct {
	Key   string `bson:"_id"`
	Value string `bson:"value"`
}

type customDb struct {
	client *mongo.Client
	coll   *mongo.Collection
}

func newCustomDb() (*customDb, error) {
	client, err := mongo.Connect(options.Client().ApplyURI(mongoUri).SetServerSelectionTimeout(mongoTimeout))
	if err != nil {
		return nil, fmt.Errorf("failed to create MongoDB client: %v", err)
	}

	ctx, cancel := mongoContext()
	defer cancel()

	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, fmt.Errorf("failed to connect to MongoDB at %s: %v", mongoUri, err)
	}

	return &customDb{
		client: client,
		coll:   client.Database(mongoDatabase).Collection(mongoCollection),
	}, nil
}

func mongoContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), mongoTimeout)
}

func mongoKeyFilter(key string) bson.D {
	return bson.D{{Key: "_id", Value: key}}
}

func (c *customDb) Release() error {
	ctx, cancel := mongoContext()
	defer cancel()

	return c.client.Disconnect(ctx)
}

func (c *customDb) Save(key, value string) error {
	return c.SaveAll(map[string]string{key: value})
}

func (c *customDb) SaveAll(data map[string]string) error {
	if len(data) == 0 {
		return nil
	}

	models := make([]mongo.WriteModel, 0, len(data))
	for key, value := range data {
		models = append(models, mongo.NewReplaceOneModel().
			SetFilter(mongoKeyFilter(key)).
			SetReplacement(mongoDocument{Key: key, Value: value}).
			SetUpsert(true))
	}

	ctx, cancel := mongoContext()
	defer cancel()

	if _, err := c.coll.BulkWrite(ctx, models); err != nil {
		return fmt.Errorf("failed to save data: %v", err)
	}
	return nil
}

func (c *customDb) Exist(key string) (bool, error) {
	ctx, cancel := mongoContext()
	defer cancel()

	count, err := c.coll.CountDocuments(ctx, mongoKeyFilter(key), options.Count().SetLimit(1))
	if err != nil {
		return false, fmt.Errorf("failed to check key %s: %v", key, err)
	}
	return count > 0, nil
}

func (c *customDb) Load(key string) (string, error) {
	ctx, cancel := mongoContext()
	defer cancel()

	var doc mongoDocument
	if err := c.coll.FindOne(ctx, mongoKeyFilter(key)).Decode(&doc); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return "", fmt.Errorf("key %s not found", key)
		}
		return "", fmt.Errorf("failed to load key %s: %v", key, err)
	}
	return doc.Value, nil
}

func (c *customDb) LoadAll() (map[string]string, error) {
	ctx, cancel := mongoContext()
	defer cancel()

	cursor, err := c.coll.Find(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("failed to load all data: %v", err)
	}

	var docs []mongoDocument
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("failed to decode all data: %v", err)
	}

	result := make(map[string]string, len(docs))
	for _, doc := range docs {
		result[doc.Key] = doc.Value
	}
	return result, nil
}

func (c *customDb) Remove(key string) error {
	ctx, cancel := mongoContext()
	defer cancel()

	if _, err := c.coll.DeleteOne(ctx, mongoKeyFilter(key)); err != nil {
		return fmt.Errorf("failed to remove key %s: %v", key, err)
	}
	return nil
}

func (c *customDb) RemoveAll() error {
	ctx, cancel := mongoContext()
	defer cancel()

	if _, err := c.coll.DeleteMany(ctx, bson.D{}); err != nil {
		return fmt.Errorf("failed to remove all data: %v", err)
	}
	return nil
}
