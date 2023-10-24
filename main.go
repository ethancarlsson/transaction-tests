package main

import (
	"database/sql"
	"os"

	"transactions.tests/transactions/simulations"
)

func main() {
	switch arg := os.Args[1]; arg {
	case "dirty_read":
		db := simulations.GetDB()
		tx, err := db.Begin()

		if err != nil {
			panic(err)
		}

		_, err = tx.Exec("INSERT INTO counters (counter) VALUES (0);")

		if err != nil {
			panic(err)
		}

		err = tx.Rollback()

		if err != nil {
			panic(err)
		}

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
		println("please choose one of: dirty_read")
	}
}
