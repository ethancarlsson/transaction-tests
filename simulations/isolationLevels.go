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

	userNum := 2
	wg.Add(userNum)

	for i := 0; i < userNum; i++ {
		go func(i int) {
			defer wg.Done()
			ctx := context.Background()
			tx, err := db.BeginTx(
				ctx,
				&sql.TxOptions{
					Isolation: isolationLevel,
					ReadOnly:  false,
				})

			if err != nil {
				panic("couldn't init transaction " + err.Error())
			}

			userIncrementsValue(tx, i)

			err = tx.Commit()
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

type DbInterfacer interface {
	Exec(query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
}

func userIncrementsValue(db DbInterfacer, userId int) {
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

func fetchCounter(db DbInterfacer) (int, error) {
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
	res.Close()

	return *counter, err
}

func readNextLine(res *sql.Rows) (*int, error) {
	res.Next()
	initialCounterVal := 0
	var counter *int = &initialCounterVal

	err := res.Scan(counter)

	return counter, err
}

func readNextLineString(res *sql.Rows) (*string, error) {
	res.Next()
	initialStringVal := ""
	var counter *string = &initialStringVal

	err := res.Scan(counter)

	return counter, err
}
