package simulations

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var cached_db *sql.DB

func GetDB() *sql.DB {
	if cached_db != nil {
		return cached_db
	}

	db, err := sql.Open("mysql", "root:example@tcp(localhost:8083)/ddia")

	if err != nil {
		panic(fmt.Sprintf("Failed to open the database. %s", err))
	}

	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	cached_db = db

	return db
}
