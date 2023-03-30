package main

import (
	"log"
	"net/http"
	"os"
	"context"
	"os/signal"
	"fmt"
	"sync"
	"syscall"

	"github.com/rs/cors"
	"github.com/go-chi/chi"
	"github.com/Shopify/sarama"
	"github.com/vektah/gqlparser/v2/gqlerror"
	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/playground"
	"github.com/99designs/gqlgen/graphql"
	
	
	"github.com/glyphack/graphlq-golang/graph"
	"github.com/glyphack/graphlq-golang/graph/generated"
	database "github.com/glyphack/graphlq-golang/internal/pkg/db/mysql"

	kingpin "github.com/alecthomas/kingpin/v2"

)

const defaultPort = "8080"

type NewUser struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

var (
	brokerList        = kingpin.Flag("brokerList", "List of brokers to connect").Default("localhost:9092").Strings()
	topic             = kingpin.Flag("topic", "Topic name").Default("test1_reply").String()
	partition         = kingpin.Flag("partition", "Partition number").Default("0").String()
	offsetType        = kingpin.Flag("offsetType", "Offset Type (OffsetNewest | OffsetOldest)").Default("-1").Int()
	messageCountStart = kingpin.Flag("messageCountStart", "Message counter start from:").Int()
)

func main() {

	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}

	router := chi.NewRouter()
	router.Use(cors.New(cors.Options{
		AllowedOrigins:   []string{"http://localhost:8080"},
		AllowCredentials: true,
		Debug:            false,
	}).Handler)

	// router.Use(auth.Middleware())

	database.InitDB()
	defer database.CloseDB()
	database.Migrate()
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

	var wg sync.WaitGroup
	done := make(chan struct{})

	// Start Kafka consumer goroutine
	// Set up a Kafka consumer in a separate goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		go func() {
			for {
				select {
				case err := <-consumer.Errors():
					log.Println(err)
				case msg := <-consumer.Messages():
					*messageCountStart++
					log.Println("Received messages", string(msg.Key), string(msg.Value))
				case <-done:
					fmt.Println("Kafka consumer stopped")
					return
				}
			}
		}()

		log.Println("Processed", *messageCountStart, "messages")

	}()


	// Start the GraphQL server in a separate goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		server := handler.NewDefaultServer(generated.NewExecutableSchema(generated.Config{Resolvers: &graph.Resolver{}}))

		server.SetErrorPresenter(func(ctx context.Context, e error) *gqlerror.Error {
			err := graphql.DefaultErrorPresenter(ctx, e)
			return err
		})

		router.Handle("/", playground.Handler("GraphQL playground", "/query"))
		router.Handle("/query", server)

		log.Printf("connect to http://localhost:%s/ for GraphQL playground", port)

		errors := http.ListenAndServe(":"+port, router)

		if errors != nil {
			log.Fatal(errors)
		}
	}()

	// Block the main goroutine to keep the program running
	select {}
	// Wait for interrupt signal to stop the program
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
	<-signalChan

	// Signal the goroutines to stop
	close(done)

	// Wait for all goroutines to finish
	wg.Wait()

	fmt.Println("Program stopped")
	
}	
