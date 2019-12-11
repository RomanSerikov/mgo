package mgo

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// DB struct for mongo client with database name
type DB struct {
	*mongo.Client

	name string
}

// NewDatabase creates DB struct with URI and database name
func NewDatabase(uri, name string) (*DB, error) {
	client, err := mongo.NewClient(options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err = client.Connect(ctx); err != nil {
		return nil, err
	}
	return &DB{client, name}, nil
}

// Close database connection
func (db *DB) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return db.Disconnect(ctx)
}

// GetItem from collection
func (db *DB) GetItem(collection string, filter interface{}, response interface{}, opts ...*options.FindOneOptions) error {
	ctx := context.Background()
	c := db.Database(db.name).Collection(collection)

	return c.FindOne(ctx, filter, opts...).Decode(response)
}

// GetItems from collection
func (db *DB) GetItems(collection string, filter interface{}, response interface{}, opts ...*options.FindOptions) error {
	ctx := context.Background()
	c := db.Database(db.name).Collection(collection)
	cur, err := c.Find(ctx, filter, opts...)
	if err != nil {
		return err
	}
	defer cur.Close(ctx)

	return cur.All(ctx, response)
}

// InsertItem in collection
func (db *DB) InsertItem(collection string, item interface{}) error {
	ctx := context.Background()
	c := db.Database(db.name).Collection(collection)
	_, err := c.InsertOne(ctx, item)
	return err
}

// InsertItems in collection
func (db *DB) InsertItems(collection string, item []interface{}) error {
	ctx := context.Background()
	c := db.Database(db.name).Collection(collection)
	_, err := c.InsertMany(ctx, item)
	return err
}

// UpdateItem in collection
func (db *DB) UpdateItem(collection string, filter bson.D, item interface{}) error {
	ctx := context.Background()
	c := db.Database(db.name).Collection(collection)
	_, err := c.UpdateOne(ctx, filter, item)
	return err
}

// UpsertItem in collection. Create if not exist, update otherwise
func (db *DB) UpsertItem(collection string, filter bson.D, item interface{}) error {
	ctx := context.Background()
	replaceOpts := options.Replace()
	replaceOpts.SetUpsert(true)

	c := db.Database(db.name).Collection(collection)
	_, err := c.ReplaceOne(ctx, filter, item, replaceOpts)
	return err
}

// DeleteItem from collection
func (db *DB) DeleteItem(collection string, filter bson.D) error {
	ctx := context.Background()
	c := db.Database(db.name).Collection(collection)
	_, err := c.DeleteOne(ctx, filter)
	return err
}

// DeleteAll the items in collection
func (db *DB) DeleteAll(collection string, filter bson.D) error {
	ctx := context.Background()
	c := db.Database(db.name).Collection(collection)
	_, err := c.DeleteMany(ctx, filter)
	return err
}

// ReplaceOne - clear all collection and insert one item in it
func (db *DB) ReplaceOne(collection string, data interface{}) error {
	if err := db.DeleteAll(collection, bson.D{}); err != nil {
		return err
	}

	if err := db.InsertItem(collection, data); err != nil {
		return err
	}
	return nil
}

// ReplaceAll - clear all collection and insert items in it
func (db *DB) ReplaceAll(collection string, data []interface{}) error {
	if len(data) == 0 {
		return nil
	}

	if err := db.DeleteAll(collection, bson.D{}); err != nil {
		return err
	}

	if err := db.InsertItems(collection, data); err != nil {
		return err
	}
	return nil
}

// CreateIndex for collection
func (db *DB) CreateIndex(collection string, keys bson.M) error {
	mod := mongo.IndexModel{
		Keys:    keys,
		Options: options.Index().SetUnique(true),
	}

	c := db.Database(db.name).Collection(collection)
	if _, err := c.Indexes().CreateOne(context.Background(), mod); err != nil {
		return errors.Wrap(err, fmt.Sprintf("collection.Indexes().CreateOne %s", collection))
	}

	log.Printf("index in collection %v for field %v created successfully.\n", collection, keys)

	return nil
}

// CreateIndices for collections
func (db *DB) CreateIndices(collections []string, keys bson.M) error {
	mod := mongo.IndexModel{
		Keys:    keys,
		Options: options.Index().SetUnique(true),
	}

	for _, column := range collections {
		collection := db.Database(db.name).Collection(column)
		if _, err := collection.Indexes().CreateOne(context.Background(), mod); err != nil {
			return errors.Wrap(err, fmt.Sprintf("collection.Indexes().CreateOne %s", column))
		}

		log.Printf("index in collection %v for field address created successfully.\n", column)
	}

	return nil
}
