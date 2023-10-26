package simulations

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"
)

func InvoiceConflictNoTrans(isSimulation bool) (string, error) {
	db := GetDB()

	var wg sync.WaitGroup

	wg.Add(2)

	// Alice
	go func() {
		defer wg.Done()

		buyer := "Alice"
		updateListingBuyer(db, buyer)
		if isSimulation {
			println("Alice: is set as the buyer")
		}

		if isSimulation {
			time.Sleep(200 * time.Millisecond)
		}

		updateInvoiceRecipient(db, buyer)

		if isSimulation {
			println("Alice: is set as the invoice recipient")
		}

		listingRecipient := fetchBuyer(db)
		invoiceRecipient := fetchInvoiceRecipient(db)

		if isSimulation {
			println("Alice: sees that " + listingRecipient + " will recieve the listing")
			println("Alice: sees that " + invoiceRecipient + " will recieve the invoice")
		}
	}()

	// Bob
	go func() {
		defer wg.Done()
		if isSimulation {
			time.Sleep(2 * time.Millisecond)
		}

		buyer := "Bob"
		updateListingBuyer(db, buyer)
		if isSimulation {
			println("Bob: is set as the buyer")
		}

		updateInvoiceRecipient(db, buyer)
		if isSimulation {
			println("Bob: is set as the invoice recipient")
		}

		listingRecipient := fetchBuyer(db)
		invoiceRecipient := fetchInvoiceRecipient(db)

		if isSimulation {
			println("Bob: sees that " + listingRecipient + " will recieve the listing")
			println("Bob: sees that " + invoiceRecipient + " will recieve the invoice")
		}

	}()

	wg.Wait()
	if isSimulation {
		println()
		println(fetchBuyer(db) + " will recieve the listing")
		println(fetchInvoiceRecipient(db) + " will recieve the invoice")
	}

	return "", nil
}

func InvoiceConflict(isolationLevel sql.IsolationLevel, isSimulation bool) (string, error) {
	db := GetDB()

	var wg sync.WaitGroup

	wg.Add(2)

	// Alice
	go func() {
		defer wg.Done()
		tx, err := db.BeginTx(context.Background(), &sql.TxOptions{
			Isolation: isolationLevel,
			ReadOnly:  false,
		})

		if err != nil {
			panic("couldn't beging transaction " + err.Error())
		}

		buyer := "Alice"
		updateListingBuyer(tx, buyer)

		if isSimulation {
			println("Alice: is set as the buyer")
		}

		if isSimulation {
			time.Sleep(200 * time.Millisecond)
		}

		updateInvoiceRecipient(tx, buyer)

		if isSimulation {
			println("Alice: is set as the invoice recipient")
		}

		listingRecipient := fetchBuyer(db)
		invoiceRecipient := fetchInvoiceRecipient(db)

		if isSimulation {
			println("Alice: sees that " + listingRecipient + " will recieve the listing before committing")
			println("Alice: sees that " + invoiceRecipient + " will recieve the invoice before committing")
		}

		if err := tx.Commit(); err != nil {
			panic("failed to commit transaction " + err.Error())
		}

	}()

	// Bob
	go func() {
		defer wg.Done()
		if isSimulation {
			time.Sleep(2 * time.Millisecond)
		}

		tx, err := db.BeginTx(context.Background(), &sql.TxOptions{
			Isolation: isolationLevel,
			ReadOnly:  false,
		})

		if err != nil {
			panic("couldn't beging transaction " + err.Error())
		}

		buyer := "Bob"
		updateListingBuyer(tx, buyer)
		if isSimulation {
			println("Bob: is set as the buyer")
		}

		updateInvoiceRecipient(tx, buyer)
		if isSimulation {
			println("Bob: is set as the invoice recipient")
		}

		listingRecipient := fetchBuyer(db)
		invoiceRecipient := fetchInvoiceRecipient(db)

		if isSimulation {
			println("Bob: sees that " + listingRecipient + " will recieve the listing before committing")
			println("Bob: sees that " + invoiceRecipient + " will recieve the invoice before committing")
		}
		if err := tx.Commit(); err != nil {
			panic("failed to commit transaction " + err.Error())
		}
	}()

	wg.Wait()

	if isSimulation {
		println()
		println(fetchBuyer(db) + " will recieve the listing")
		println(fetchInvoiceRecipient(db) + " will recieve the invoice")
	}

	return "", nil
}

func fetchBuyer(db DbInterfacer) string {
	res, err := db.Query(`
				SELECT buyer FROM listings
				WHERE id = 1234;
			`)

	if err != nil {
		panic(fmt.Errorf("couldn't open listings. %s", err.Error()))
	}

	recipient, err := readNextLineString(res)

	if recipient == nil {
		panic(fmt.Errorf("pointer to counter is nil"))
	}

	if err != nil {
		panic(fmt.Errorf("Couldn't read next line. %s", err.Error()))
	}

	defer res.Close()

	return *recipient
}

func fetchInvoiceRecipient(db DbInterfacer) string {
	res, err := db.Query(`
				SELECT recipient FROM invoices
				WHERE id = 48;
			`)

	if err != nil {
		panic(fmt.Errorf("couldn't open invoices. %s", err.Error()))
	}

	recipient, err := readNextLineString(res)

	if recipient == nil {
		panic(fmt.Errorf("pointer to counter is nil"))
	}

	if err != nil {
		panic(fmt.Errorf("Couldn't read next line. %s", err.Error()))
	}

	defer res.Close()

	return *recipient
}

func updateInvoiceRecipient(db DbInterfacer, recipient string) {
	_, err := db.Exec(`
		INSERT INTO invoices (id, recipient, listing_id) VALUES (48, ?, 1234)
		ON DUPLICATE KEY UPDATE recipient=?, listing_id=1234; 
	`,
		recipient, recipient)

	if err != nil {
		panic("failed updating buyer " + err.Error())
	}
}

func updateListingBuyer(db DbInterfacer, buyer string) {
	_, err := db.Exec(`
		UPDATE listings SET buyer = ?
		WHERE id = 1234
	`,
		buyer)

	if err != nil {
		panic("failed updating buyer " + err.Error())
	}
}
