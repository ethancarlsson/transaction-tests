package simulations

import (
	"context"
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

	db, err := sql.Open("mysql", "root:example@tcp(0.0.0.0:8083)/ddia")

	if err != nil {
		panic(fmt.Sprintf("Failed to open the database. %s", err))
	}

	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(50)

	cached_db = db

	return db
}

func RunInTransaction(db *sql.DB, isolationLevel sql.IsolationLevel, callback func(DbInterfacer, bool), isLogging bool) {
	tx, err := db.BeginTx(
		context.Background(),
		&sql.TxOptions{
			Isolation: isolationLevel,
			ReadOnly:  false,
		})

	if err != nil {
		panic("couldn't beginning transaction " + err.Error())
	}

	callback(tx, isLogging)

	if err := tx.Commit(); err != nil {
		panic("failed to commit transaction " + err.Error())
	}
}
