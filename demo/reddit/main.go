package main

import (
	"context"
	"fmt"
	"log"

	"github.com/saturnines/nexus-core/pkg/config"
	"github.com/saturnines/nexus-core/pkg/core"
)

func main() {
	// Load the YAML config
	loader := config.NewPipelineLoader(
		&config.EnvExpander{},
		&config.PipelineDefaults{},
	)

	cfg, err := loader.Load("demo/reddit/reddit.yaml")
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

	fmt.Printf("Extracted %d issues\n", len(results))

	// That's it.
	for _, issue := range results[:10] {
		num := issue["number"]
		var numStr string
		if num == nil {
			numStr = "nil" // or whatever you like
		} else {
			numStr = fmt.Sprintf("%v", num)
		}
		fmt.Printf("Issue #%s: %v by %v\n",
			numStr,
			issue["title"],
			issue["author"],
		)
	}
}
