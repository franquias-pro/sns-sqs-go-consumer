package mongodb

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"example.com/sns-sqs-go-consumer/internal/handler"
)

type EventStore struct {
	client     *mongo.Client
	collection *mongo.Collection
}

func Connect(ctx context.Context, uri, database, collection string, maxPoolSize uint64) (*EventStore, error) {
	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetMaxPoolSize(maxPoolSize))
	if err != nil { return nil, fmt.Errorf("connect MongoDB: %w", err) }
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, fmt.Errorf("ping MongoDB: %w", err)
	}
	return &EventStore{client: client, collection: client.Database(database).Collection(collection)}, nil
}

type storedEvent struct {
	ID         string    `bson:"_id"`
	Type       string    `bson:"type"`
	OccurredAt time.Time `bson:"occurred_at"`
	Payload    string    `bson:"payload_json"`
	ReceivedAt time.Time `bson:"received_at"`
}

func (s *EventStore) Save(ctx context.Context, event handler.Event, body string) (bool, error) {
	_, err := s.collection.InsertOne(ctx, storedEvent{
		ID: event.EventID, Type: event.Type, OccurredAt: event.OccurredAt,
		Payload: body, ReceivedAt: time.Now().UTC(),
	})
	if mongo.IsDuplicateKeyError(err) { return false, nil }
	if err != nil { return false, fmt.Errorf("insert event %s: %w", event.EventID, err) }
	return true, nil
}

func (s *EventStore) Ping(ctx context.Context) error { return s.client.Ping(ctx, nil) }
func (s *EventStore) Close(ctx context.Context) error { return s.client.Disconnect(ctx) }
