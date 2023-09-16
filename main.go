// microservice2.go

package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"net/http"
)

// Message Define a struct to represent the message
type Message struct {
	Text string `json:"text"`
}

// Database connection parameters
const (
	host     = "localhost"
	port     = 5432
	user     = "your_username"
	password = "your_password"
	dbname   = "your_database"
)

// Function to handle incoming messages
func receiveMessageHandler(w http.ResponseWriter, r *http.Request) {
	var message Message
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&message)
	if err != nil {
		http.Error(w, "Failed to decode message", http.StatusBadRequest)
		return
	}

	// Store the message in the PostgreSQL database
	err = saveMessage(message.Text)
	if err != nil {
		http.Error(w, "Failed to save message to the database", http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "Message received and saved: %s", message.Text)
}

// Function to save the message in the PostgreSQL database
func saveMessage(text string) error {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec("INSERT INTO messages (text) VALUES ($1)", text)
	if err != nil {
		return err
	}

	return nil
}

func main() {
	http.HandleFunc("/receive", receiveMessageHandler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
