package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"time"
)

// Config holds the service configuration
type Config struct {
	Port                   string
	MonolithURL            string
	MoviesServiceURL       string
	EventsServiceURL       string
	GradualMigration       bool
	MoviesMigrationPercent int
}

// Service represents the proxy service
type Service struct {
	config           Config
	monolithProxy    *httputil.ReverseProxy
	moviesProxy      *httputil.ReverseProxy
	eventsProxy      *httputil.ReverseProxy
	monolithURL      *url.URL
	moviesServiceURL *url.URL
	eventsServiceURL *url.URL
}

func main() {
	// Load configuration
	config := loadConfig()

	// Initialize service
	service, err := NewService(config)
	if err != nil {
		log.Fatalf("Failed to initialize service: %v", err)
	}

	// Set up routes
	http.HandleFunc("/health", service.healthHandler)
	http.HandleFunc("/api/movies", service.moviesHandler)
	http.HandleFunc("/api/movies/", service.moviesHandler)
	http.HandleFunc("/api/users", service.proxyToMonolith)
	http.HandleFunc("/api/users/", service.proxyToMonolith)
	http.HandleFunc("/api/payments", service.proxyToMonolith)
	http.HandleFunc("/api/payments/", service.proxyToMonolith)
	http.HandleFunc("/api/subscriptions", service.proxyToMonolith)
	http.HandleFunc("/api/subscriptions/", service.proxyToMonolith)
	http.HandleFunc("/api/events", service.proxyToEvents)
	http.HandleFunc("/api/events/", service.proxyToEvents)

	// Start server
	log.Printf("Starting proxy service on port %s", config.Port)
	log.Printf("Monolith URL: %s", config.MonolithURL)
	log.Printf("Movies Service URL: %s", config.MoviesServiceURL)
	log.Printf("Events Service URL: %s", config.EventsServiceURL)
	log.Printf("Gradual Migration: %v", config.GradualMigration)
	log.Printf("Movies Migration Percent: %d%%", config.MoviesMigrationPercent)
	log.Fatal(http.ListenAndServe(":"+config.Port, nil))
}

// NewService creates a new proxy service
func NewService(config Config) (*Service, error) {
	monolithURL, err := url.Parse(config.MonolithURL)
	if err != nil {
		return nil, err
	}

	moviesServiceURL, err := url.Parse(config.MoviesServiceURL)
	if err != nil {
		return nil, err
	}

	eventsServiceURL, err := url.Parse(config.EventsServiceURL)
	if err != nil {
		return nil, err
	}

	return &Service{
		config:           config,
		monolithProxy:    httputil.NewSingleHostReverseProxy(monolithURL),
		moviesProxy:      httputil.NewSingleHostReverseProxy(moviesServiceURL),
		eventsProxy:      httputil.NewSingleHostReverseProxy(eventsServiceURL),
		monolithURL:      monolithURL,
		moviesServiceURL: moviesServiceURL,
		eventsServiceURL: eventsServiceURL,
	}, nil
}

func loadConfig() Config {
	// Default values
	config := Config{
		Port:                   "8000",
		MonolithURL:            "http://localhost:8080",
		MoviesServiceURL:       "http://localhost:8081",
		EventsServiceURL:       "http://localhost:8082",
		GradualMigration:       false,
		MoviesMigrationPercent: 0,
	}

	// Override with environment variables
	if port := os.Getenv("PORT"); port != "" {
		config.Port = port
	}
	if monolithURL := os.Getenv("MONOLITH_URL"); monolithURL != "" {
		config.MonolithURL = monolithURL
	}
	if moviesServiceURL := os.Getenv("MOVIES_SERVICE_URL"); moviesServiceURL != "" {
		config.MoviesServiceURL = moviesServiceURL
	}
	if eventsServiceURL := os.Getenv("EVENTS_SERVICE_URL"); eventsServiceURL != "" {
		config.EventsServiceURL = eventsServiceURL
	}
	if gradualMigration := os.Getenv("GRADUAL_MIGRATION"); gradualMigration == "true" {
		config.GradualMigration = true
	}
	if migrationPercent := os.Getenv("MOVIES_MIGRATION_PERCENT"); migrationPercent != "" {
		if percent, err := strconv.Atoi(migrationPercent); err == nil {
			config.MoviesMigrationPercent = percent
		}
	}

	return config
}

func (s *Service) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "healthy",
		"config": map[string]interface{}{
			"gradual_migration":        s.config.GradualMigration,
			"movies_migration_percent": s.config.MoviesMigrationPercent,
		},
	})
}

func (s *Service) moviesHandler(w http.ResponseWriter, r *http.Request) {
	// Check if we should use gradual migration
	if s.config.GradualMigration {
		// Generate random number between 0 and 100
		rand.Seed(time.Now().UnixNano())
		roll := rand.Intn(100)

		// If roll is less than migration percent, use movies service
		if roll < s.config.MoviesMigrationPercent {
			log.Printf("[Movies Handler] Routing to Movies Service (roll: %d < %d)", roll, s.config.MoviesMigrationPercent)
			s.moviesProxy.ServeHTTP(w, r)
			return
		}
		log.Printf("[Movies Handler] Routing to Monolith (roll: %d >= %d)", roll, s.config.MoviesMigrationPercent)
	}

	// Default to monolith
	log.Printf("[Movies Handler] Routing to Monolith (gradual migration disabled or condition not met)")
	s.proxyToMonolith(w, r)
}

func (s *Service) proxyToMonolith(w http.ResponseWriter, r *http.Request) {
	// Log the proxied request
	log.Printf("[Monolith Proxy] %s %s", r.Method, r.URL.Path)

	// Use the reverse proxy
	s.monolithProxy.ServeHTTP(w, r)
}

func (s *Service) proxyToEvents(w http.ResponseWriter, r *http.Request) {
	// Log the proxied request
	log.Printf("[Events Proxy] %s %s", r.Method, r.URL.Path)

	// Use the reverse proxy
	s.eventsProxy.ServeHTTP(w, r)
}