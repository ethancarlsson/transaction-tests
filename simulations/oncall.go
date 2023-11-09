package simulations

import (
	"database/sql"
	"fmt"
	"sync"
)

const ALICE_ID = 1
const BOB_ID = 2

func NoTOnCall(isSimulation bool) (string, error) {

	db := GetDB()

	var wg sync.WaitGroup
	wg.Add(2)

	// Alice
	go func() {
		defer wg.Done()

		attemptToGoOffCall(1, db, isSimulation)
	}()

	// Bob
	go func() {
		defer wg.Done()

		attemptToGoOffCall(2, db, isSimulation)
	}()

	wg.Wait()

	if isSimulation {
		println(
			fmt.Sprintf("%d doctors on call", queryOnCallCount(db)),
		)
	}

	return "", nil
}

func OnCall(isolationLevel sql.IsolationLevel, isSimulation bool) (string, error) {

	db := GetDB()

	var wg sync.WaitGroup
	wg.Add(2)

	// Alice
	go func() {
		defer wg.Done()

		RunInTransaction(
			db,
			isolationLevel,
			func(db DbInterfacer, isLogging bool) {
				attemptToGoOffCall(1, db, isLogging)
			},
			isSimulation,
		)
	}()

	// Bob
	go func() {
		defer wg.Done()

		RunInTransaction(
			db,
			isolationLevel,
			func(db DbInterfacer, isLogging bool) {
				attemptToGoOffCall(2, db, isLogging)
			},
			isSimulation,
		)
	}()

	wg.Wait()

	if isSimulation {
		println(
			fmt.Sprintf("%d doctors on call", queryOnCallCount(db)),
		)
	}

	return "", nil
}

func attemptToGoOffCall(doctor_id int, db DbInterfacer, isLogging bool) {
	onCallCount := queryOnCallCount(db)

	if isLogging {
		println(fmt.Sprintf("doctor %d sees there are %d doctors on call including them", doctor_id, onCallCount))
	}

	if onCallCount < 2 {
		if isLogging {
			println(fmt.Sprintf("doctor %d stays on call because there are less than 2 doctors on call at the moment", doctor_id))
		}
		return
	}

	updateDoctorOnCallStatus(doctor_id, false, db)

	if isLogging {
		println(fmt.Sprintf("doctor %d sets themselves off call", doctor_id))
	}
}

func updateDoctorOnCallStatus(doctor_id int, on_call bool, db DbInterfacer) {
	_, err := db.Exec(`
		UPDATE doctors
		SET on_call = ?
		WHERE id = ?
		AND shift_id = 1234
	`, on_call, doctor_id)

	if err != nil {
		panic("couldn't set doctor off call " + err.Error())
	}
}

func queryOnCallCount(db DbInterfacer) int {
	rows, err := db.Query(`
		SELECT COUNT(*) as count from doctors
		WHERE on_call = true
		AND shift_id = 1234
	`)

	if err != nil || rows == nil {
		panic("couldn't get on call count")
	}

	return scanOnCallCount(rows)
}

func scanOnCallCount(rows *sql.Rows) int {
	rows.Next()
	count := 0

	err := rows.Scan(&count)
	if err != nil {
		panic("couldn't read id " + err.Error())
	}

	defer rows.Close()

	return count
}
