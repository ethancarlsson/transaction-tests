package simulations

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
)

func CounterSimulationWithIsolationLevel(isolationLevel sql.IsolationLevel) (string, error) {

	db := GetDB()

	var wg sync.WaitGroup

	userNum := 5
	wg.Add(userNum)

	for i := 0; i < userNum; i++ {
		go func(i int) {
			defer wg.Done()

			tx, err := db.BeginTx(
				context.Background(),
				&sql.TxOptions{
					Isolation: isolationLevel,
					ReadOnly:  false,
				})

			// tx, err := db.Begin()

			if err != nil {
				panic("couldn't init transaction " + err.Error())
			}

			userIncrementsValue(db, i)

			err = tx.Rollback()

			if err != nil {
				panic("couln't commit " + err.Error())
			}
		}(i)
	}

	wg.Wait()

	counter, err := fetchCounter(db)

	if err != nil {
		return "", err
	}

	println(fmt.Sprintf("Counter value now equals %d", counter))

	return isolationLevel.String() + " simulation complete", nil
}

func NoTransaction() (string, error) {
	db := GetDB()

	var wg sync.WaitGroup

	userNum := 20
	wg.Add(userNum)

	for i := 0; i < userNum; i++ {
		go func(i int) {
			defer wg.Done()
			userIncrementsValue(db, i)
		}(i)
	}

	wg.Wait()

	counter, err := fetchCounter(db)

	if err != nil {
		return "", err
	}

	println(fmt.Sprintf("Counter value now equals %d", counter))

	return "No transaction simulation complete", nil
}

func userIncrementsValue(db *sql.DB, userId int) {
	println(fmt.Sprintf("User %d: gets counter", userId))

	counter, err := fetchCounter(db)

	if err != nil {
		panic("error fetching counter " + err.Error())
	}

	println(fmt.Sprintf("User %d: counter value = %d", userId, counter))

	// counter += 1

	_, err = db.Exec(`
		UPDATE counters
		SET counter = counter+1
		WHERE id = 1;
	`)

	if err != nil {
		panic(fmt.Errorf("couldn't update counter. %s", err.Error()))
	}

	println(fmt.Sprintf("User %d: increments counter", userId))

	if err != nil {
		panic("couln't set counter " + err.Error())
	}
}

func setCounter(db *sql.DB, counter int) error {
	_, err := db.Exec(`
		UPDATE counters
		SET counter = ?
		WHERE id = 1;
	`, counter)

	if err != nil {
		return fmt.Errorf("couldn't update counter. %s", err.Error())
	}

	return nil
}

func fetchCounter(db *sql.DB) (int, error) {
	res, err := db.Query(`
		SELECT counter FROM counters
		WHERE id = 1;
	`)

	if err != nil {
		return -1, fmt.Errorf("couldn't open counters db. %s", err.Error())
	}

	counter, err := readNextLine(res)

	if counter == nil {
		return -1, fmt.Errorf("pointer to counter is nil")
	}

	return *counter, err
}

func readNextLine(res *sql.Rows) (*int, error) {
	res.Next()
	initialCounterVal := 0
	var counter *int = &initialCounterVal

	err := res.Scan(counter)

	return counter, err
}
