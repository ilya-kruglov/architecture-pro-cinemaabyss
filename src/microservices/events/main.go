package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

// Config holds the service configuration
type Config struct {
	Port         string
	KafkaBrokers string
}

// Service represents the events service
type Service struct {
	config      Config
	writer      *kafka.Writer
	reader      *kafka.Reader
	topics      []string
	cancelFunc  context.CancelFunc
}

// Event represents a generic event
type Event struct {
	ID        string      `json:"id"`
	Type      string      `json:"type"`
	Timestamp time.Time   `json:"timestamp"`
	Payload   interface{} `json:"payload"`
}

// MovieEvent represents a movie-related event
type MovieEvent struct {
	MovieID     int      `json:"movie_id"`
	Title       string   `json:"title"`
	Action      string   `json:"action"`
	UserID      int      `json:"user_id,omitempty"`
	Rating      float64  `json:"rating,omitempty"`
	Genres      []string `json:"genres,omitempty"`
	Description string   `json:"description,omitempty"`
}

// UserEvent represents a user-related event
type UserEvent struct {
	UserID    int       `json:"user_id"`
	Username  string    `json:"username,omitempty"`
	Email     string    `json:"email,omitempty"`
	Action    string    `json:"action"`
	Timestamp time.Time `json:"timestamp"`
}

// PaymentEvent represents a payment-related event
type PaymentEvent struct {
	PaymentID  int       `json:"payment_id"`
	UserID     int       `json:"user_id"`
	Amount     float64   `json:"amount"`
	Status     string    `json:"status"`
	Timestamp  time.Time `json:"timestamp"`
	MethodType string    `json:"method_type,omitempty"`
}

// EventResponse represents the response after publishing an event
type EventResponse struct {
	Status    string `json:"status"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Event     Event  `json:"event"`
}

func main() {
	// Load configuration
	config := loadConfig()

	// Initialize service
	service, err := NewService(config)
	if err != nil {
		log.Fatalf("Failed to initialize service: %v", err)
	}
	defer service.Close()

	// Set up HTTP routes
	http.HandleFunc("/api/events/health", service.healthHandler)
	http.HandleFunc("/api/events/movie", service.movieEventHandler)
	http.HandleFunc("/api/events/user", service.userEventHandler)
	http.HandleFunc("/api/events/payment", service.paymentEventHandler)

	// Start consumer in background
	ctx, cancel := context.WithCancel(context.Background())
	service.cancelFunc = cancel
	go service.startConsumer(ctx)

	// Start server
	server := &http.Server{
		Addr:    ":" + config.Port,
		Handler: nil,
	}

	// Graceful shutdown
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan

		log.Println("Shutting down gracefully...")
		cancel()
		
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := server.Shutdown(ctx); err != nil {
			log.Printf("Server forced to shutdown: %v", err)
		}
	}()

	log.Printf("Starting events service on port %s", config.Port)
	log.Printf("Kafka brokers: %s", config.KafkaBrokers)
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("Server failed to start: %v", err)
	}

	log.Println("Server stopped")
}

func loadConfig() Config {
	config := Config{
		Port:         "8082",
		KafkaBrokers: "localhost:9092",
	}

	if port := os.Getenv("PORT"); port != "" {
		config.Port = port
	}
	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		config.KafkaBrokers = brokers
	}

	return config
}

// NewService creates a new events service
func NewService(config Config) (*Service, error) {
	topics := []string{"movie-events", "user-events", "payment-events"}

	// Create writer for producing messages
	writer := &kafka.Writer{
		Addr:         kafka.TCP(config.KafkaBrokers),
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
	}

	service := &Service{
		config: config,
		writer: writer,
		topics: topics,
	}

	return service, nil
}

// Close closes the service connections
func (s *Service) Close() {
	if s.writer != nil {
		s.writer.Close()
	}
	if s.reader != nil {
		s.reader.Close()
	}
	if s.cancelFunc != nil {
		s.cancelFunc()
	}
}

func (s *Service) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Check Kafka connectivity
	kafkaStatus := "connected"
	if s.writer == nil {
		kafkaStatus = "disconnected"
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": true,
		"kafka":  kafkaStatus,
		"topics": s.topics,
	})
}

func (s *Service) movieEventHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var movieEvent MovieEvent
	if err := json.NewDecoder(r.Body).Decode(&movieEvent); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	// Create event
	event := Event{
		ID:        fmt.Sprintf("movie-%d-%s-%d", movieEvent.MovieID, movieEvent.Action, time.Now().UnixNano()),
		Type:      "movie",
		Timestamp: time.Now(),
		Payload:   movieEvent,
	}

	// Publish to Kafka
	response, err := s.publishEvent("movie-events", event)
	if err != nil {
		log.Printf("Failed to publish movie event: %v", err)
		http.Error(w, fmt.Sprintf("Failed to publish event: %v", err), http.StatusInternalServerError)
		return
	}

	log.Printf("Movie event published: movie_id=%d, action=%s, partition=%d, offset=%d",
		movieEvent.MovieID, movieEvent.Action, response.Partition, response.Offset)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

func (s *Service) userEventHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var userEvent UserEvent
	if err := json.NewDecoder(r.Body).Decode(&userEvent); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	// Set timestamp if not provided
	if userEvent.Timestamp.IsZero() {
		userEvent.Timestamp = time.Now()
	}

	// Create event
	event := Event{
		ID:        fmt.Sprintf("user-%d-%s-%d", userEvent.UserID, userEvent.Action, time.Now().UnixNano()),
		Type:      "user",
		Timestamp: userEvent.Timestamp,
		Payload:   userEvent,
	}

	// Publish to Kafka
	response, err := s.publishEvent("user-events", event)
	if err != nil {
		log.Printf("Failed to publish user event: %v", err)
		http.Error(w, fmt.Sprintf("Failed to publish event: %v", err), http.StatusInternalServerError)
		return
	}

	log.Printf("User event published: user_id=%d, action=%s, partition=%d, offset=%d",
		userEvent.UserID, userEvent.Action, response.Partition, response.Offset)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

func (s *Service) paymentEventHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var paymentEvent PaymentEvent
	if err := json.NewDecoder(r.Body).Decode(&paymentEvent); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	// Set timestamp if not provided
	if paymentEvent.Timestamp.IsZero() {
		paymentEvent.Timestamp = time.Now()
	}

	// Create event
	event := Event{
		ID:        fmt.Sprintf("payment-%d-%s-%d", paymentEvent.PaymentID, paymentEvent.Status, time.Now().UnixNano()),
		Type:      "payment",
		Timestamp: paymentEvent.Timestamp,
		Payload:   paymentEvent,
	}

	// Publish to Kafka
	response, err := s.publishEvent("payment-events", event)
	if err != nil {
		log.Printf("Failed to publish payment event: %v", err)
		http.Error(w, fmt.Sprintf("Failed to publish event: %v", err), http.StatusInternalServerError)
		return
	}

	log.Printf("Payment event published: payment_id=%d, status=%s, partition=%d, offset=%d",
		paymentEvent.PaymentID, paymentEvent.Status, response.Partition, response.Offset)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

func (s *Service) publishEvent(topic string, event Event) (EventResponse, error) {
	// Serialize event
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return EventResponse{}, fmt.Errorf("failed to serialize event: %v", err)
	}

	// Create message WITHOUT setting Topic in the message
	msg := kafka.Message{
		Key:   []byte(event.ID),
		Value: eventJSON,
		Time:  event.Timestamp,
	}

	// Set writer topic for this message
	s.writer.Topic = topic

	// Write message
	err = s.writer.WriteMessages(context.Background(), msg)
	if err != nil {
		return EventResponse{}, fmt.Errorf("failed to write message: %v", err)
	}

	// Note: With kafka-go, we don't get partition and offset directly from WriteMessages
	// In a production environment, you might want to use a different approach
	return EventResponse{
		Status:    "success",
		Partition: 0, // Placeholder
		Offset:    0, // Placeholder
		Event:     event,
	}, nil
}

func (s *Service) startConsumer(ctx context.Context) {
	// Create a reader for each topic and multiplex
	for _, topic := range s.topics {
		go s.consumeTopic(ctx, topic)
	}
}

func (s *Service) consumeTopic(ctx context.Context, topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{s.config.KafkaBrokers},
		Topic:    topic,
		GroupID:  "events-service",
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
		MaxWait:  1 * time.Second,
	})

	defer reader.Close()

	log.Printf("Consumer started for topic: %s", topic)

	for {
		select {
		case <-ctx.Done():
			log.Printf("Consumer stopped for topic: %s", topic)
			return
		default:
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				if err == context.Canceled {
					return
				}
				log.Printf("Error reading message from topic %s: %v", topic, err)
				continue
			}

			s.processMessage(topic, msg)
		}
	}
}

func (s *Service) processMessage(topic string, msg kafka.Message) {
	log.Printf("Received message from topic %s at offset %d: %s", topic, msg.Offset, string(msg.Value))

	// Parse event
	var event Event
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		log.Printf("Failed to parse event: %v", err)
		return
	}

	// Log event details based on type
	switch event.Type {
	case "movie":
		var movieEvent MovieEvent
		data, _ := json.Marshal(event.Payload)
		json.Unmarshal(data, &movieEvent)
		log.Printf("[Consumer] Movie event processed: movie_id=%d, action=%s", movieEvent.MovieID, movieEvent.Action)
	case "user":
		var userEvent UserEvent
		data, _ := json.Marshal(event.Payload)
		json.Unmarshal(data, &userEvent)
		log.Printf("[Consumer] User event processed: user_id=%d, action=%s", userEvent.UserID, userEvent.Action)
	case "payment":
		var paymentEvent PaymentEvent
		data, _ := json.Marshal(event.Payload)
		json.Unmarshal(data, &paymentEvent)
		log.Printf("[Consumer] Payment event processed: payment_id=%d, status=%s", paymentEvent.PaymentID, paymentEvent.Status)
	}
}