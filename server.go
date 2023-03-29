package main

import (
	"log"
	"net/http"
	"os"

	"github.com/glyphack/graphlq-golang/graph"
	"github.com/glyphack/graphlq-golang/graph/generated"
	// "github.com/glyphack/graphlq-golang/internal/auth"
	// _ "github.com/glyphack/graphlq-golang/internal/auth"
	database "github.com/glyphack/graphlq-golang/internal/pkg/db/mysql"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/playground"
	"github.com/go-chi/chi"
)

const defaultPort = "8080"

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}

	router := chi.NewRouter()
	// Add CORS middleware around every request
	// See https://github.com/rs/cors for full option listing
	router.Use(cors.New(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowCredentials: true,
		Debug:            true,
	}).Handler)

	// router.Use(auth.Middleware())
	
	database.InitDB()
	defer database.CloseDB()
	database.Migrate()
	server := handler.NewDefaultServer(generated.NewExecutableSchema(generated.Config{Resolvers: &graph.Resolver{}}))
	router.Handle("/", playground.Handler("GraphQL playground", "/query"))
	router.Handle("/query", server)

	log.Printf("connect to http://localhost:%s/ for GraphQL playground", port)

	err := http.ListenAndServe(":"+port, router)

	if err != nil {
		log.Fatal(err)
	}
}
