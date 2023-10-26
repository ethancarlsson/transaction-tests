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

func main() {
	db := simulations.GetDB()
	_, err := db.Exec("CREATE TABLE IF NOT EXISTS counters (id INT PRIMARY KEY, counter INT NOT NULL);")

	if err != nil {
		panic("couldn't create table " + err.Error())
	}

	switch arg := os.Args[1]; arg {
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
		println("please choose one of: count100")
	}
}
