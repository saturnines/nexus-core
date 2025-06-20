package main

import (
	"context"
	"fmt"
	"github.com/joho/godotenv"
	"github.com/saturnines/nexus-core/pkg/config"
	"github.com/saturnines/nexus-core/pkg/core"
	"log"
)

func main() {

	if err := godotenv.Load(); err != nil {
		log.Println(".env file not loaded:", err)
	}

	// Load the YAML config
	loader := config.NewPipelineLoader(
		&config.EnvExpander{},
		&config.PipelineDefaults{},
	)

	cfg, err := loader.Load("demo/stripe-simple/stripe.yaml")
	if err != nil {
		log.Fatal(err)
	}

	// Create connector from config
	connector, err := core.NewConnector(cfg.(*config.Pipeline))
	if err != nil {
		log.Fatal(err)
	}

	// Extract data
	results, err := connector.Extract(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Extracted %d customers\n", len(results))

	// That's it.
	for _, customer := range results[:3] {
		fmt.Printf("Customer: %v (%v)\n", customer["name"], customer["email"])
	}
}
