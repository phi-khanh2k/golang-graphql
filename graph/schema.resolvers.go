package graph

// This file will be automatically regenerated based on the schema, any resolver implementations
// will be copied through when generating and any unknown code will be moved to the end.

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"
	"os"
	"os/signal"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/glyphack/graphlq-golang/graph/generated"
	"github.com/glyphack/graphlq-golang/graph/model"
	"github.com/glyphack/graphlq-golang/internal/auth"
	"github.com/glyphack/graphlq-golang/internal/links"
	"github.com/glyphack/graphlq-golang/internal/users"
	"github.com/glyphack/graphlq-golang/pkg/jwt"

	kingpin "github.com/alecthomas/kingpin/v2"

	"github.com/Shopify/sarama"
)


var (
	brokerList        = kingpin.Flag("brokerList", "List of brokers to connect").Default("localhost:9092").Strings()
	topic             = kingpin.Flag("topic", "Topic name").Default("test1_reply").String()
	partition         = kingpin.Flag("partition", "Partition number").Default("0").String()
	offsetType        = kingpin.Flag("offsetType", "Offset Type (OffsetNewest | OffsetOldest)").Default("-1").Int()
	messageCountStart = kingpin.Flag("messageCountStart", "Message counter start from:").Int()
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

func (r *mutationResolver) CreateUser(ctx context.Context, input model.NewUser) (string, error) {
	broker := "localhost:9092"
    topic1 := "test1_reply"

    // Create a producer
    producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})
    if err != nil {
        panic(err)
    }

    // // Create a consumer
    // consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
    //     "bootstrap.servers": broker,
    //     "group.id":          "my-group",
    //     "auto.offset.reset": "earliest",
    // })
    // if err != nil {
    //     panic(err)
    // }

    // // Subscribe to the topic
    // consumer.SubscribeTopics([]string{topic1}, nil)

	// Set up a signal handler to stop the consumer when the program receives a SIGINT or SIGTERM signal
    // sigchan := make(chan os.Signal, 1)
    // signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
    // go func() {
    //     <-sigchan
    //     fmt.Println("Received signal, shutting down consumer...")
    //     consumer.Close()
    // }()

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
	kingpin.Parse()
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	brokers := *brokerList
	master, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Panic(err)
	}
	defer func() {
		if err := master.Close(); err != nil {
			log.Panic(err)
		}
	}()
	consumer, err := master.ConsumePartition(*topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Panic(err)
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	doneCh := make(chan struct{})
	go func() {
		log.Println("Processed")

		for {
		log.Println("Processed11")

			select {
			case err := <-consumer.Errors():
				log.Println(err)
			case msg := <-consumer.Messages():
				*messageCountStart++
				log.Println("Received messages", string(msg.Key), string(msg.Value))
				consumer.Close()
				doneCh <- struct{}{}
				return
			case <-signals:
				log.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
		return
	}()
	<-doneCh
	log.Println("Processed", *messageCountStart, "messages")
	// =================================================================

	return "sdad", nil
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

// !!! WARNING !!!
// The code below was going to be deleted when updating resolvers. It has been copied here so you have
// one last chance to move it out of harms way if you want. There are two reasons this happens:
//   - When renaming or deleting a resolver the old code will be put in here. You can safely delete
//     it when you're done.
//   - You have helper methods in this file. Move them out to keep these resolver files clean.
func CreateTopic(topicName string) {
	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		panic(err)
	}

	defer admin.Close()

	maxDur, err := time.ParseDuration("60s")
	if err != nil {
		panic("ParseDuration(60s)")
	}

	ctx := context.Background()
	// Check if the topic exists
	exists, err := topicExists(admin, topicName)
	if exists {
		fmt.Println("Topic already exists")
		return
	}

	results, err := admin.CreateTopics(
		ctx,
		// Multiple topics can be created simultaneously
		// by providing more TopicSpecification structs here.
		[]kafka.TopicSpecification{{
			Topic: topicName,
		}},
		// Admin options
		kafka.SetAdminOperationTimeout(maxDur))
	if err != nil {
		log.Printf("Failed to create topic: %v\n", err)
	}

	log.Println("results:", results)

}

func topicExists(admin *kafka.AdminClient, topic string) (bool, error) {
	meta, err := admin.GetMetadata(&topic, false, 1000)
	if err != nil {
		return false, err
	}

	if _, ok := meta.Topics[topic]; ok {
		return true, nil
	}

	return false, nil
}

type Consumer struct{}

func (c *Consumer) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("Message topic:%q partition:%d offset:%d message:%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Value)

		// Mark message as consumed
		session.MarkMessage(msg, "")
	}

	return nil
}

// Mutation returns generated.MutationResolver implementation.
func (r *Resolver) Mutation() generated.MutationResolver { return &mutationResolver{r} }

// Query returns generated.QueryResolver implementation.
func (r *Resolver) Query() generated.QueryResolver { return &queryResolver{r} }

type mutationResolver struct{ *Resolver }
type queryResolver struct{ *Resolver }
