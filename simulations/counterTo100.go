package simulations

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
)

func CountTo100(isolationLevel sql.IsolationLevel, isLogging bool) (string, error) {
	db := GetDB()

	var wg sync.WaitGroup

	wg.Add(2)

	// Writer
	go func() {
		defer wg.Done()
		RunInTransaction(
			db,
			isolationLevel,
			func(di DbInterfacer, b bool) {
				for i := 0; i < 10000; i++ {
					if isLogging {
						println("Writer: increments by one")
					}

					_, err := di.Exec(`
						UPDATE counters
						SET counter = counter+1
						WHERE id = 42;
					`)

					if err != nil {
						panic(fmt.Errorf("couldn't update counter. %s", err.Error()))
					}
				}
			},
			isLogging,
		)
	}()

	// Reader
	go func() {
		defer wg.Done()

		RunInTransaction(
			db, isolationLevel,
			func(di DbInterfacer, b bool) {
				for i := 0; i < 10000; i++ {
					ReadCount42(di, isLogging)
				}
			},
			isLogging)

	}()

	wg.Wait()

	ReadCount42(db, isLogging)

	return isolationLevel.String() + " simulation complete", nil
}

func NoTTwoWritersCountTo100(isLogging bool) (string, error) {
	db := GetDB()

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()

		for i := 0; i < 10000; i++ {
			if isLogging {
				println("Writer1: increments by one")
			}

			_, err := db.Exec(`
				UPDATE counters
				SET counter = counter+1
				WHERE id = 42;
			`)

			if err != nil {
				panic(fmt.Errorf("couldn't update counter. %s", err.Error()))
			}
		}
	}()

	go func() {
		defer wg.Done()

		for i := 0; i < 10000; i++ {
			if isLogging {
				println("Writer2: increments by one")
			}

			_, err := db.Exec(`
				UPDATE counters
				SET counter = counter+1
				WHERE id = 42;
			`)

			if err != nil {
				panic(fmt.Errorf("couldn't update counter. %s", err.Error()))
			}
		}
	}()

	wg.Wait()

	ReadCount42(db, isLogging)

	return "No transaction simulation complete", nil
}

func TwoWritersCountTo100(isolationLevel sql.IsolationLevel, isLogging bool) (string, error) {
	db := GetDB()

	var wg sync.WaitGroup

	wg.Add(2)

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

		for i := 0; i < 10000; i++ {
			if isLogging {
				println("Writer: increments by one")
			}

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

		for i := 0; i < 10000; i++ {
			if isLogging {
				println("Writer: increments by one")
			}

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

	wg.Wait()

	ReadCount42(db, isLogging)

	return isolationLevel.String() + " simulation complete", nil
}

func ReadCount42(dbInter DbInterfacer, isLogging bool) {
	res, err := dbInter.Query(`
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

	if isLogging {
		println(fmt.Sprintf("Reader: sees counter val: %d", *counter))
	}

	res.Close()

	if err != nil {
		panic(fmt.Errorf("couldn't update counter. %s", err.Error()))
	}
}

func CountTo100NoTrans(isLogging bool) (string, error) {
	db := GetDB()

	var wg sync.WaitGroup

	wg.Add(2)

	// Writer
	go func() {
		defer wg.Done()

		for i := 0; i < 10000; i++ {
			if isLogging {
				println("Writer: increments by one")
			}

			_, err := db.Exec(`
				UPDATE counters
				SET counter = counter+1
				WHERE id = 42;
			`)

			if err != nil {
				panic(fmt.Errorf("couldn't update counter. %s", err.Error()))
			}
		}
	}()

	// Reader
	go func() {
		defer wg.Done()

		for i := 0; i < 10000; i++ {
			ReadCount42(db, isLogging)
		}
	}()

	wg.Wait()

	ReadCount42(db, isLogging)

	return "no transaction simulation complete", nil
}
