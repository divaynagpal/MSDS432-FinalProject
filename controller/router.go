package main

import (
	"MS432Project/utils"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

var (
	db  *sql.DB
	err error
)

type App struct {
	DB *sql.DB
}

var (
	//Hostname = "34.31.231.237"
	Hostname = "34.126.208.89"
	//Port     = "3306"
	Port     = "5432"
	Username = "divay"
	Password = "root"
	//Database = "test"
	Database = "ms432project"
)

func main() {
	fmt.Println("Starting new Go project...")

	mux := http.NewServeMux()

	s := &http.Server{
		Addr:         "0.0.0.0:8080",
		Handler:      mux,
		IdleTimeout:  10 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	//dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", Username, Password, Hostname, Port, Database)
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", Hostname, Port, Username, Password, Database)
	db, err = sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("Could not connect to the database: %v", err)
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		log.Fatalf("Could not ping the database: %v", err)
	}
	app := &utils.App{DB: db}
	mux.Handle("/report1", http.HandlerFunc(app.GetReport1))
	mux.Handle("/report2", http.HandlerFunc(app.GetReport2))
	mux.Handle("/report3", http.HandlerFunc(app.GetReport3))
	mux.Handle("/report4a", http.HandlerFunc(app.GetReport4a))
	mux.Handle("/report4b", http.HandlerFunc(app.GetReport4b))
	mux.Handle("/report4c", http.HandlerFunc(app.GetReport4c))
	mux.Handle("/report5", http.HandlerFunc(app.GetReport5))
	mux.Handle("/report6", http.HandlerFunc(app.GetReport6))
	mux.Handle("/report7", http.HandlerFunc(app.GetReport7))

	fmt.Println("Ready to serve at :8080")
	err := s.ListenAndServe()
	if err != nil {
		log.Fatalf("ListenAndServe: %v", err)
	}
}
