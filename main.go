// Microservice 2: main.go

package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/streadway/amqp"
	"log"
)

// Message Define a struct to represent the message
type Message struct {
	Text string `json:"text"`
}

// RabbitMQ connection parameters
const (
	rabbitMQURL = "amqp://guest:guest@localhost:5672/"
	queueName   = "messages"
)

// Database connection parameters
const (
	host     = "localhost"
	port     = "5432"
	user     = "postgres"
	password = "password"
	dbname   = "core-service-db"
)

func main() {
	consumeFromRabbitMQ()
}

func consumeFromRabbitMQ() {
	conn, err := amqp.Dial(rabbitMQURL)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err	}
	defer func(conn *amqp.Connection) {
		_ = conn.Close()
	}(conn)

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer func(ch *amqp.Channel) {
		_ = ch.Close()
	}(ch)

	q, err := ch.QueueDeclare(
		queueName, // Queue name
		false,     // Durable
		false,     // Delete when unused
		false,     // Exclusive
		false,     // No-wait
		nil,       // Arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	msgs, err := ch.Consume(
		q.Name, // Queue
		"",     // Consumer
		true,   // Auto-acknowledge
		false,  // Exclusive
		false,  // No-local
		false,  // No-wait
		nil,    // Args
	)
	if err != nil {
		log.Fatalf("Failed to register a consumer: %v", err)
	}

	for msg := range msgs {
		var message Message
		if err := json.Unmarshal(msg.Body, &message); err != nil {
			log.Printf("Failed to decode message: %v", err)
			continue
		}

		// Store the message in the PostgreSQL database
		if err := saveMessage(message.Text); err != nil {
			log.Printf("Failed to save message to the database: %v", err)
			continue
		}
		log.Printf("Message received and saved: %s", message.Text)
	}
}

func saveMessage(text string) error {
	// Connect to the PostgreSQL server
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Cannot connect to postgres server")
		return err
	}
	defer db.Close()
	// Check if the database already exists
	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS core-service-db")
	fmt.Printf("Database exists!")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Database created (if not existed) successfully.")
	// Add message
	_, err = db.Exec("INSERT INTO messages (text) VALUES ($1)", text)
	if err != nil {
		return err
	}
	return nil
}
