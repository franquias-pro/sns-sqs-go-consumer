package mongodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/mamartins1997/sns-sqs-go-consumer/internal/domain"
)

type EventRepository struct {
	client    *mongo.Client
	events    *mongo.Collection
	deletions *mongo.Collection
}

func Connect(ctx context.Context, uri, database, collection string, maxPoolSize uint64) (*EventRepository, error) {
	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetMaxPoolSize(maxPoolSize))
	if err != nil { return nil, fmt.Errorf("connect MongoDB: %w", err) }
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, fmt.Errorf("ping MongoDB: %w", err)
	}
	db := client.Database(database)
	return &EventRepository{
		client: client, events: db.Collection(collection), deletions: db.Collection(collection + "_deletions"),
	}, nil
}

type storedEvent struct {
	ID         string    `bson:"_id"`
	OrderID    string    `bson:"order_id"`
	Type       string    `bson:"type"`
	OccurredAt time.Time `bson:"occurred_at"`
	Payload    string    `bson:"payload_json"`
	ReceivedAt time.Time `bson:"received_at"`
}

type deletionMarker struct {
	OrderID   string    `bson:"_id"`
	DeletedAt time.Time `bson:"deleted_at"`
}

func (r *EventRepository) Create(ctx context.Context, evt domain.Event, orderID, body string) (bool, error) {
	deleted, err := r.wasDeleted(ctx, orderID)
	if err != nil { return false, err }
	if deleted { return false, nil }

	_, err = r.events.InsertOne(ctx, storedEvent{
		ID: evt.EventID, OrderID: orderID, Type: evt.Type, OccurredAt: evt.OccurredAt,
		Payload: body, ReceivedAt: time.Now().UTC(),
	})
	inserted := err == nil
	if err != nil && !mongo.IsDuplicateKeyError(err) {
		return false, fmt.Errorf("insert event %s: %w", evt.EventID, err)
	}

	// A delete may arrive between the first check and insert. The second
	// check prevents concurrent creates from resurrecting a deleted order.
	deleted, err = r.wasDeleted(ctx, orderID)
	if err != nil { return false, err }
	if deleted {
		if _, err := r.events.DeleteMany(ctx, bson.D{{Key: "order_id", Value: orderID}}); err != nil {
			return false, fmt.Errorf("remove events for deleted order %s: %w", orderID, err)
		}
		return false, nil
	}
	return inserted, nil
}

func (r *EventRepository) DeleteByOrderID(ctx context.Context, orderID string) (int64, error) {
	// The marker survives retries and prevents a late order.created from
	// recreating the order. A failed delete is retried by SQS.
	_, err := r.deletions.InsertOne(ctx, deletionMarker{OrderID: orderID, DeletedAt: time.Now().UTC()})
	if err != nil && !mongo.IsDuplicateKeyError(err) {
		return 0, fmt.Errorf("mark order %s deleted: %w", orderID, err)
	}
	result, err := r.events.DeleteMany(ctx, bson.D{{Key: "order_id", Value: orderID}})
	if err != nil { return 0, fmt.Errorf("delete events for order %s: %w", orderID, err) }
	return result.DeletedCount, nil
}

func (r *EventRepository) wasDeleted(ctx context.Context, orderID string) (bool, error) {
	var marker deletionMarker
	err := r.deletions.FindOne(ctx, bson.D{{Key: "_id", Value: orderID}}).Decode(&marker)
	if errors.Is(err, mongo.ErrNoDocuments) { return false, nil }
	if err != nil { return false, fmt.Errorf("check deletion marker for order %s: %w", orderID, err) }
	return true, nil
}

func (r *EventRepository) Ping(ctx context.Context) error { return r.client.Ping(ctx, nil) }
func (r *EventRepository) Close(ctx context.Context) error { return r.client.Disconnect(ctx) }
