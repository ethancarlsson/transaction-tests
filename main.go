package main

import (
	"database/sql"
	"os"

	"transactions.tests/transactions/simulations"
)

func main() {
	switch arg := os.Args[1]; arg {
	case "linearizable":
		res, err := simulations.PrepDirtyReadTable()
		println(res)

		if err != nil {
			println("Error", err.Error())
			return
		}

		resDirty, err := simulations.CounterSimulationWithIsolationLevel(sql.LevelLinearizable)

		if err != nil {
			println("Error", err.Error())
			return
		}

		println("res: ", resDirty)
	case "serializable":
		res, err := simulations.PrepDirtyReadTable()
		println(res)

		if err != nil {
			println("Error", err.Error())
			return
		}

		resDirty, err := simulations.CounterSimulationWithIsolationLevel(sql.LevelSerializable)

		if err != nil {
			println("Error", err.Error())
			return
		}

		println("res: ", resDirty)
	case "repeatable_read":
		res, err := simulations.PrepDirtyReadTable()
		println(res)

		if err != nil {
			println("Error", err.Error())
			return
		}

		resDirty, err := simulations.CounterSimulationWithIsolationLevel(sql.LevelRepeatableRead)

		if err != nil {
			println("Error", err.Error())
			return
		}

		println("res: ", resDirty)
	case "read_committed":
		res, err := simulations.PrepDirtyReadTable()
		println(res)

		if err != nil {
			println("Error", err.Error())
			return
		}

		resDirty, err := simulations.CounterSimulationWithIsolationLevel(sql.LevelReadCommitted)

		if err != nil {
			println("Error", err.Error())
			return
		}

		println("res: ", resDirty)
	case "read_uncommitted":
		res, err := simulations.PrepDirtyReadTable()
		println(res)

		if err != nil {
			println("Error", err.Error())
			return
		}

		resDirty, err := simulations.CounterSimulationWithIsolationLevel(sql.LevelReadUncommitted)

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
