package main

import (
	"database/sql"
	"fmt"
	"os"

	"transactions.tests/transactions/simulations"
)

const NO_TRANSACTION = 444

func readIsolationLevel() sql.IsolationLevel {
	level := sql.LevelDefault

	if len(os.Args) < 3 {
		panic("choose one of: read_uncommitted, read_committed, write_committed, repeatable_read, snapshot, serializable, linearizable")
	}

	switch arg2 := os.Args[2]; arg2 {
	case "read_uncommitted":
		level = sql.LevelReadUncommitted
	case "read_committed":
		level = sql.LevelReadCommitted
	case "write_committed":
		level = sql.LevelWriteCommitted
	case "repeatable_read":
		level = sql.LevelRepeatableRead
	case "snapshot":
		level = sql.LevelSnapshot
	case "serializable":
		level = sql.LevelSerializable
	case "linearizable":
		level = sql.LevelLinearizable
	case "no_t":
		level = NO_TRANSACTION
	default:
		panic("choose one of: read_uncommitted, read_committed, write_committed, repeatable_read, snapshot, serializable, linearizable")
	}

	return level
}

func createTables(db *sql.DB) {

	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS counters (id INT PRIMARY KEY, counter INT NOT NULL);
	`)

	if err != nil {
		panic("couldn't create table " + err.Error())
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS listings (id INT PRIMARY KEY, buyer TEXT NOT NULL);
	`)

	if err != nil {
		panic("couldn't create table " + err.Error())
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS invoices (
			id INT PRIMARY KEY,
			recipient TEXT NOT NULL,
			listing_id INT NOT NULL,
			CONSTRAINT fk_category FOREIGN KEY (listing_id) 
				REFERENCES listings(id)

		);
	`)

	if err != nil {
		panic("couldn't create table " + err.Error())
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS accounts (
			id INT PRIMARY KEY AUTO_INCREMENT,
			balance INT NOT NULL
		);
	`)

	if err != nil {
		panic("couldn't create table " + err.Error())
	}
}

func main() {
	db := simulations.GetDB()

	createTables(db)

	switch arg := os.Args[1]; arg {
	case "inconsistentacc":
		_, err := db.Exec(`
			INSERT INTO accounts VALUES (1, 500)
			ON DUPLICATE KEY UPDATE balance=500; 
		`)

		if err != nil {
			panic(fmt.Errorf("Failed to insert new account. %s", err.Error()))
		}

		_, err = db.Exec(`
			INSERT INTO accounts VALUES (2, 500)
			ON DUPLICATE KEY UPDATE balance=500; 
		`)

		if err != nil {
			panic(fmt.Errorf("Failed to insert new account. %s", err.Error()))
		}

		level := readIsolationLevel()
		var res string
		var e error

		if level == NO_TRANSACTION {
		res, e = simulations.NoTCheckBalanceDuringTransfer(true)
		} else {
		res, e = simulations.CheckBalanceDuringTransfer(level, true)
		}

		if e != nil {
			panic(e)
		}

		println(res)

	case "invoiceconflict":
		_, err := db.Exec(`
			INSERT INTO listings VALUES (1234, "NO_ONE")
			ON DUPLICATE KEY UPDATE buyer="NO_ONE"; 
		`)

		if err != nil {
			panic(fmt.Errorf("Failed to insert new listing. %s", err.Error()))
		}

		_, err = db.Exec(`
			INSERT INTO invoices (id, recipient, listing_id) VALUES (48, "NO_ONE", 1234)
			ON DUPLICATE KEY UPDATE recipient="NO_ONE", listing_id=1234; 
		`)

		if err != nil {
			panic(fmt.Errorf("Failed to insert new invoice. %s", err.Error()))
		}

		level := readIsolationLevel()
		var res string
		var e error

		if level == NO_TRANSACTION {
			res, e = simulations.InvoiceConflictNoTrans(true)
		} else {
			res, e = simulations.InvoiceConflict(level, true)
		}

		if e != nil {
			panic(e)
		}

		println(res)
	case "twowriterscount":
		_, err := db.Exec(`
			INSERT INTO counters VALUES (42, 0)
			ON DUPLICATE KEY UPDATE counter=0; 
		`)

		if err != nil {
			panic(fmt.Errorf("Failed to insert new counter. %s", err.Error()))
		}
		level := readIsolationLevel()
		var res string
		var e error

		if level == NO_TRANSACTION {
			res, e = simulations.NoTTwoWritersCountTo100(true)
		} else {
			res, e = simulations.TwoWritersCountTo100(level, true)
		}

		if e != nil {
			panic(e)
		}

		println(res)

	case "count100":
		_, err := db.Exec(`
			INSERT INTO counters VALUES (42, 0)
			ON DUPLICATE KEY UPDATE counter=0; 
		`)

		if err != nil {
			panic(fmt.Errorf("Failed to insert new counter. %s", err.Error()))
		}
		level := readIsolationLevel()
		var res string
		var e error

		if level == NO_TRANSACTION {
			res, e = simulations.CountTo100NoTrans(true)
		} else {
			res, e = simulations.CountTo100(level, true)
		}

		if e != nil {
			panic(e)
		}

		println(res)

	case "transaction_level":
		res, err := simulations.PrepDirtyReadTable()
		println(res)

		if err != nil {
			println("Error", err.Error())
			return
		}

		level := sql.LevelDefault

		if len(os.Args) < 3 {
			panic("choose one of: read_uncommitted, read_committed, write_committed, repeatable_read, snapshot, serializable, linearizable")
		}

		switch arg2 := os.Args[2]; arg2 {
		case "read_uncommitted":
			level = sql.LevelReadUncommitted
		case "read_committed":
			level = sql.LevelReadCommitted
		case "write_committed":
			level = sql.LevelWriteCommitted
		case "repeatable_read":
			level = sql.LevelRepeatableRead
		case "snapshot":
			level = sql.LevelSnapshot
		case "serializable":
			level = sql.LevelSerializable
		case "linearizable":
			level = sql.LevelLinearizable
		default:
			panic("choose one of: read_uncommitted, read_committed, write_committed, repeatable_read, snapshot, serializable, linearizable")
		}

		resDirty, err := simulations.CounterSimulationWithIsolationLevel(level)

		if err != nil {
			println("Error", err.Error())
			return
		}

		println("res: ", resDirty)
	case "no_trans":
		res, err := simulations.PrepDirtyReadTable()
		println(res)

		if err != nil {
			println("Error", err.Error())
			return
		}

		resDirty, err := simulations.NoTransaction()

		if err != nil {
			println("Error", err.Error())
			return
		}

		println("res: ", resDirty)
	default:
		println("please choose one of: count100, invoiceconflict, inconsistentacc")
	}
}
