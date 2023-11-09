package simulations

import (
	"database/sql"
	"fmt"
	"sync"
	"testing"
)

func BenchmarkReadCommittedCount10000(b *testing.B) {
	GetDB()
	for i := 0; i < b.N; i++ {
		_, _ = CountTo100(sql.LevelReadCommitted, false)
	}
}

func BenchmarkReadUncommittedCount10000(b *testing.B) {
	GetDB()
	for i := 0; i < b.N; i++ {
		_, _ = CountTo100(sql.LevelReadUncommitted, false)
	}
}

func BenchmarkNoTransaction10000(b *testing.B) {
	GetDB()
	for i := 0; i < b.N; i++ {
		_, _ = CountTo100NoTrans(false)
	}
}

func writeReadCommitted(db *sql.DB, wg *sync.WaitGroup) {
	// Writer
	go func() {
		defer wg.Done()
		RunInTransaction(
			db,
			sql.LevelReadCommitted,
			func(di DbInterfacer, isLogging bool) {
				for i := 0; i < 10000; i++ {
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
			false,
		)
	}()
}

func readWithLevel(db *sql.DB, wg *sync.WaitGroup, isolationLevel sql.IsolationLevel) {
	defer wg.Done()

	RunInTransaction(
		db,
		isolationLevel,
		func(di DbInterfacer, isLogging bool) {
			for i := 0; i < 10000; i++ {
				ReadCount42(di, isLogging)
			}
		},
		false)

}

func BenchmarkReadUncommittedCount10000ReaderAndCommittedWriter(b *testing.B) {
	db := GetDB()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup

		wg.Add(2)

		// writer
		go writeReadCommitted(db, &wg)

		// Reader
		go readWithLevel(db, &wg, sql.LevelReadUncommitted)

		wg.Wait()

		ReadCount42(db, false)
	}
}

func BenchmarkReadCommittedCount10000ReaderAndCommittedWriter(b *testing.B) {
	db := GetDB()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup

		wg.Add(2)

		// writer
		go writeReadCommitted(db, &wg)

		// Reader
		go readWithLevel(db, &wg, sql.LevelReadCommitted)

		wg.Wait()

		ReadCount42(db, false)
	}
}

func BenchmarkReadNoT10000ReaderAndCommittedWriter(b *testing.B) {
	db := GetDB()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup

		wg.Add(2)

		// writer
		go writeReadCommitted(db, &wg)

		// Reader
		go func() {
			defer wg.Done()
			for i := 0; i < 10000; i++ {
				ReadCount42(db, false)
			}

		}()

		wg.Wait()

		ReadCount42(db, false)
	}
}
