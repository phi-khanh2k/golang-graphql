package graph

// This file will be automatically regenerated based on the schema, any resolver implementations
// will be copied through when generating and any unknown code will be moved to the end.

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/glyphack/graphlq-golang/graph/generated"
	"github.com/glyphack/graphlq-golang/graph/model"
	"github.com/glyphack/graphlq-golang/internal/auth"
	"github.com/glyphack/graphlq-golang/internal/links"
	"github.com/glyphack/graphlq-golang/internal/users"
	"github.com/glyphack/graphlq-golang/pkg/jwt"
)

func (r *mutationResolver) CreateLink(ctx context.Context, input model.NewLink) (*model.Link, error) {
	user := auth.ForContext(ctx)
	if user == nil {
		return &model.Link{}, fmt.Errorf("access denied")
	}
	var link links.Link
	link.Title = input.Title
	link.Address = input.Address
	link.User = user
	linkId := link.Save()
	grahpqlUser := &model.User{
		ID:   user.ID,
		Name: user.Username,
	}
	return &model.Link{ID: strconv.FormatInt(linkId, 10), Title: link.Title, Address: link.Address, User: grahpqlUser}, nil
}

func (r *mutationResolver) Login(ctx context.Context, input model.Login) (string, error) {
	var user users.User
	user.Username = input.Username
	user.Password = input.Password
	correct := user.Authenticate()
	if !correct {
		return "", &users.WrongUsernameOrPasswordError{}
	}
	token, err := jwt.GenerateToken(user.Username)
	if err != nil {
		return "", err
	}
	return token, nil
}

func (r *mutationResolver) RefreshToken(ctx context.Context, input model.RefreshTokenInput) (string, error) {
	username, err := jwt.ParseToken(input.Token)
	if err != nil {
		return "", fmt.Errorf("access denied")
	}
	token, err := jwt.GenerateToken(username)
	if err != nil {
		return "", err
	}
	return token, nil
}

func (r *queryResolver) Links(ctx context.Context) ([]*model.Link, error) {
	var resultLinks []*model.Link
	var dbLinks []links.Link
	dbLinks = links.GetAll()
	for _, link := range dbLinks {
		grahpqlUser := &model.User{
			Name: link.User.Password,
		}
		resultLinks = append(resultLinks, &model.Link{ID: link.ID, Title: link.Title, Address: link.Address, User: grahpqlUser})
	}
	return resultLinks, nil
}

func (r *mutationResolver) CreateUser(ctx context.Context, input model.NewUser) (string, error) {
	broker := "localhost:9092"
	topic1 := "test1"

	// Create a producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})
	if err != nil {
		panic(err)
	}
	// Produce a message to the topic
	value := "hello, world"
	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic1, Partition: 0},
		Value:          []byte(value),
	}, nil)
	if err != nil {
		panic(err)
	}
	producer.Close()

	// =================================================================
	config := &kafka.ConfigMap{
		"bootstrap.servers":                     "localhost:9092",
		"group.id":                              "test-group",
		"go.application.rebalance.enable":       true,
		"acks":                                  "all",
		"max.in.flight.requests.per.connection": 1,
	}

	// Create consumer
	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	topic := "test1"

	partition := 0
	tp := kafka.TopicPartition{Topic: &topic, Partition: int32(partition)}
	err = consumer.Assign([]kafka.TopicPartition{tp})
	if err != nil {
		panic(err)
	}

	partitions, err := consumer.Assignment()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Consumer is assigned to partitions: %v\n", partitions)

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	run := true
	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:

			// Poll for messages
			ev, err := consumer.ReadMessage(-1)
			if err != nil {
				fmt.Println("Timeout")
				continue
			}
			fmt.Println(ev)

			if ev == nil {
				continue
			}

			com, err := consumer.CommitOffsets([]kafka.TopicPartition{{Topic: ev.TopicPartition.Topic, Partition: ev.TopicPartition.Partition, Offset: ev.TopicPartition.Offset}})
			fmt.Println(com, err)
			// switch e :=ev {
			// case *kafka.Message:
			// 	fmt.Printf("Received message on partition %d: %s\n", e.TopicPartition.Partition, string(e.Value))

			// 	// Commit the message offset
			// 	offset := e.TopicPartition.Offset
			// 	fmt.Println(e.TopicPartition.Topic, e.TopicPartition.Partition, offset)

			// 	com, err := consumer.CommitMessage(e)
			// 	fmt.Println(com, err)

			// case kafka.Error:
			// 	fmt.Fprintf(os.Stderr, "Error: %v\n", e)
			// 	if e.Code() == kafka.ErrAllBrokersDown {
			// 		run = false
			// 	}
			// default:
			// 	fmt.Printf("Ignored event: %s\n", ev)
			// }
		}
	}

	consumer.Close()

	return "sdad", nil
}

// Mutation returns generated.MutationResolver implementation.
func (r *Resolver) Mutation() generated.MutationResolver { return &mutationResolver{r} }

// Query returns generated.QueryResolver implementation.
func (r *Resolver) Query() generated.QueryResolver { return &queryResolver{r} }

type mutationResolver struct{ *Resolver }
type queryResolver struct{ *Resolver }
