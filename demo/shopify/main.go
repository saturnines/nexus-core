package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	"github.com/saturnines/nexus-core/pkg/config"
	"github.com/saturnines/nexus-core/pkg/core"
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

	cfg, err := loader.Load("demo/shopify/shopify.yaml")
	if err != nil {
		log.Fatal(err)
	}

	// Create connector and extract
	connector, err := core.NewConnector(cfg.(*config.Pipeline))
	if err != nil {
		log.Fatal("Failed to create connector:", err)
	}

	results, err := connector.Extract(context.Background())
	if err != nil {
		log.Fatal("Failed to extract:", err)
	}

	// Save to JSON
	file, _ := os.Create("products.json")
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	encoder.Encode(results)
	file.Close()

	fmt.Printf("Extracted %d products → products.json\n", len(results))
}
