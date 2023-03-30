package main

import (
	"log"
	"net/http"
	"os"
	"fmt"
	"encoding/json"

	"github.com/rs/cors"
	"github.com/glyphack/graphlq-golang/graph"
	"github.com/glyphack/graphlq-golang/graph/generated"
	// "github.com/glyphack/graphlq-golang/internal/auth"
	// _ "github.com/glyphack/graphlq-golang/internal/auth"
	database "github.com/glyphack/graphlq-golang/internal/pkg/db/mysql"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/playground"
	"github.com/go-chi/chi"
	"github.com/glyphack/graphlq-golang/internal/users"
	"github.com/confluentinc/confluent-kafka-go/kafka"

)

const defaultPort = "8080"

type NewUser struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}

	router := chi.NewRouter()
	// Add CORS middleware around every request
	// See https://github.com/rs/cors for full option listing
	router.Use(cors.New(cors.Options{
		AllowedOrigins:   []string{"http://localhost:8080"},
		AllowCredentials: true,
		Debug:            false,
	}).Handler)

	// router.Use(auth.Middleware())

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		log.Fatal(err)
	}

	c.SubscribeTopics([]string{"test1_reply"}, nil)
	log.Printf("connect to http://localhost:%s/ for GraphQL playground", c)
	for {
		msg, err := c.ReadMessage(-1)

		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
			var user1 users.User

			err := json.Unmarshal(msg.Value, &user1)
			if err != nil {
				log.Println("=> error converting event object:", err)
			}

		} else {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

	database.InitDB()
	defer database.CloseDB()
	database.Migrate()
	server := handler.NewDefaultServer(generated.NewExecutableSchema(generated.Config{Resolvers: &graph.Resolver{}}))
	router.Handle("/", playground.Handler("GraphQL playground", "/query"))
	router.Handle("/query", server)

	log.Printf("connect to http://localhost:%s/ for GraphQL playground", port)

	err1 := http.ListenAndServe(":"+port, router)

	if err1 != nil {
		log.Fatal(err1)
	}

	
}
