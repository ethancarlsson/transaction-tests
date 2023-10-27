package simulations

import (
	"database/sql"
	"fmt"
	"sync"
	"time"
)

func CheckBalanceDuringTransfer(isolationLevel sql.IsolationLevel, isSimulation bool) (string, error) {
	db := GetDB()

	var wg sync.WaitGroup

	wg.Add(2)

	// Reader
	go func() {
		defer wg.Done()
		RunInTransaction(db, isolationLevel, CheckAccounts, isSimulation)
	}()

	// Transferer
	go func() {
		defer wg.Done()

		if isSimulation {
			time.Sleep(time.Millisecond)
		}

		RunInTransaction(db, isolationLevel, Transfer, isSimulation)
	}()

	wg.Wait()

	return "", nil
}

func NoTCheckBalanceDuringTransfer(isSimulation bool) (string, error) {
	db := GetDB()

	var wg sync.WaitGroup
	wg.Add(2)

	// Reader
	go func() {
		defer wg.Done()
		CheckAccounts(db, isSimulation)
	}()

	// Transferer
	go func() {
		defer wg.Done()

		if isSimulation {
			time.Sleep(time.Millisecond)
		}

		Transfer(db, isSimulation)
	}()

	wg.Wait()
	return "", nil
}

func Transfer(db DbInterfacer, isLogging bool) {
	_, err := db.Exec(`
		UPDATE accounts 
		SET balance = balance + 100
		WHERE id = 1
	`)

	if err != nil {
		panic(fmt.Errorf("Failed to increase balance by 100. %s", err.Error()))
	}
	if isLogging {
		println("$100 added to account 1")
	}

	_, err = db.Exec(`
		UPDATE accounts 
		SET balance = balance - 100
		WHERE id = 2
	`)

	if err != nil {
		panic(fmt.Errorf("Failed to decrease balance by 100. %s", err.Error()))
	}

	if isLogging {
		println("$100 removed from account 2")
	}
}

func TransferBack(db DbInterfacer, isLogging bool) {
	_, err := db.Exec(`
		UPDATE accounts 
		SET balance = balance - 100
		WHERE id = 1
	`)

	if err != nil {
		panic(fmt.Errorf("Failed to decrease balance by 100. %s", err.Error()))
	}
	if isLogging {
		println("$100 removed from account 1")
	}

	_, err = db.Exec(`
		UPDATE accounts 
		SET balance = balance + 100
		WHERE id = 2
	`)

	if err != nil {
		panic(fmt.Errorf("Failed to increase balance by 100. %s", err.Error()))
	}

	if isLogging {
		println("$100 added to account 2")
	}
}

func CheckAccounts(db DbInterfacer, isSimulation bool) {
	totalBalance := 0
	for i := 1; i <= 2; i++ {
		res, err := db.Query(`
		SELECT * FROM accounts WHERE id = ?;
	`, i)
		if err != nil {
			panic("couldn't query accounts. " + err.Error())
		}

		if res == nil {
			panic("result is nil, should be 2 rows")
		}

		id, balance := scanAccBalance(res)

		totalBalance += balance

		if isSimulation {
			println(fmt.Sprintf("Reader: account %d has a balance of %d", id, balance))
		}

		if isSimulation {
			time.Sleep(time.Second)
		}
	}

	if isSimulation {
		println(fmt.Sprintf("Reader: total balance is %d", totalBalance))
	}
}

func scanAccBalance(rows *sql.Rows) (int, int) {
	rows.Next()
	id := 0
	balance := 0

	err := rows.Scan(&id, &balance)
	if err != nil {
		panic("couldn't read id " + err.Error())
	}

	defer rows.Close()

	return id, balance
}

