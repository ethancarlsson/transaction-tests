package simulations

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
)

func DirtyRead(isolationLevel sql.IsolationLevel) (string, error) {

	db := GetDB()

	var wg sync.WaitGroup

	wg.Add(2)

	_, err := db.Exec(`
		INSERT INTO counters VALUES (42, 0)
		ON DUPLICATE KEY UPDATE counter=0; 
	`)

	if err != nil {
		panic(fmt.Errorf("Failed to inser new counter. %s", err.Error()))
	}

	// Writer
	go func() {
		defer wg.Done()

		tx, err := db.BeginTx(
			context.Background(),
			&sql.TxOptions{
				Isolation: isolationLevel,
				ReadOnly:  false,
			})

		if err != nil {
			panic("couldn't init transaction " + err.Error())
		}


		for i := 0; i < 100; i++ {
			_, err = tx.Exec(`
				UPDATE counters
				SET counter = counter+1
				WHERE id = 42;
			`)

			if err != nil {
				panic(fmt.Errorf("couldn't update counter. %s", err.Error()))
			}
		}

		err = tx.Commit()

		if err != nil {
			panic("couln't commit " + err.Error())
		}
	}()

	// Reader
	go func() {
		defer wg.Done()

		tx, err := db.BeginTx(
			context.Background(),
			&sql.TxOptions{
				Isolation: isolationLevel,
				ReadOnly:  false,
			})

		if err != nil {
			panic("couldn't init transaction " + err.Error())
		}

		for i := 0; i < 100; i++ {
			res, err := tx.Query(`
				SELECT counter FROM counters
				WHERE id = 42;
			`)


			if err != nil {
				panic(fmt.Errorf("couldn't open counters db. %s", err.Error()))
			}

			counter, err := readNextLine(res)

			if counter == nil {
				panic(fmt.Errorf("pointer to counter is nil"))
			}

			println(fmt.Sprintf("counter val: %d", *counter))

			res.Close()

			if err != nil {
				panic(fmt.Errorf("couldn't update counter. %s", err.Error()))
			}
		}

		err = tx.Commit()

		if err != nil {
			panic("couln't commit " + err.Error())
		}
	}()

	wg.Wait()

	res, err := db.Query(`
				SELECT counter FROM counters
				WHERE id = 42;
			`)

	if err != nil {
		panic(fmt.Errorf("couldn't open counters db. %s", err.Error()))
	}

	counter, err := readNextLine(res)

	if counter == nil {
		panic(fmt.Errorf("pointer to counter is nil"))
	}

	if err != nil {
		return "", err
	}

	println(fmt.Sprintf("Counter value now equals %d", *counter))

	return isolationLevel.String() + " simulation complete", nil
}
