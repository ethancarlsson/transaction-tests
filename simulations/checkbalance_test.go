package simulations

import (
	"database/sql"
	"testing"
)

func BenchmarkRepeatableReadCheckBalanceDuringTransfer(b *testing.B) {
	GetDB()
	for i := 0; i < b.N*100; i++ {
		_, _ = CheckBalanceDuringTransfer(sql.LevelRepeatableRead, false)
	}
}

func BenchmarkReadCommittedCheckBalanceDuringTransfer(b *testing.B) {
	GetDB()
	for i := 0; i < b.N*100; i++ {
		_, _ = CheckBalanceDuringTransfer(sql.LevelReadCommitted, false)
	}
}

func BenchmarkReadUncommittedCheckBalanceDuringTransfer(b *testing.B) {
	GetDB()
	for i := 0; i < b.N*100; i++ {
		_, _ = CheckBalanceDuringTransfer(sql.LevelReadUncommitted, false)
	}
}

func BenchmarkNoTCheckBalanceDuringTransfer(b *testing.B) {
	GetDB()
	for i := 0; i < b.N*100; i++ {
		_, _ = NoTCheckBalanceDuringTransfer(false)
	}
}

func BenchmarkReaderNoTCheckBalanceDuringTransfer(b *testing.B) {
	db := GetDB()
	// Write while benchmarking a go routine is transfering money back and forth
	quit := make(chan bool)
	go func() {
		for {
			if <- quit {
				break
			}

			Transfer(db, false)
			TransferBack(db, false)
		}
	}()

	for i := 0; i < b.N; i++ {
		for i := 0; i < 1000; i++ {
			CheckAccounts(db, false)
		}
	}
	quit <- true // need to break when test is done so otherwise earlier 
	// transactions always seem like they're faster no matter that transaction level
}

func BenchmarkReaderReadCommittedCheckBalanceDuringTransfer(b *testing.B) {
	db := GetDB()
	// Write while benchmarking a go routine is transfering money back and forth
	quit := make(chan bool)
	go func() {
		for {
			if <- quit {
				break
			}
			RunInTransaction(
				db,
				sql.LevelReadCommitted,
				func(db DbInterfacer, isLogging bool) {
					Transfer(db, isLogging)
					TransferBack(db, isLogging)
				},
				false,
			)
		}
	}()
	for i := 0; i < b.N; i++ {
		for i := 0; i < 1000; i++ {
			RunInTransaction(db, sql.LevelReadCommitted, CheckAccounts, false)
		}
	}

	quit <- true // need to break when test is done so otherwise earlier 
	// transactions always seem like they're faster no matter that transaction level
}

func BenchmarkReaderRepeatableReadCheckBalanceDuringTransfer(b *testing.B) {
	db := GetDB()
	// Write while benchmarking a go routine is transfering money back and forth
	quit := make(chan bool)
	go func() {
		for {
			if <- quit {
				break
			}
			RunInTransaction(
				db,
				sql.LevelRepeatableRead,
				func(db DbInterfacer, isLogging bool) {
					Transfer(db, isLogging)
					TransferBack(db, isLogging)
				},
				false,
			)
		}
	}()

	for i := 0; i < b.N; i++ {
		for i := 0; i < 1000; i++ {
			RunInTransaction(db, sql.LevelRepeatableRead, CheckAccounts, false)
		}
	}
	quit <- true // need to break when test is done so otherwise earlier 
	// transactions always seem like they're faster no matter that transaction level
}


func BenchmarkReaderReadUncommittedCheckBalanceDuringTransfer(b *testing.B) {
	db := GetDB()

	// Write while benchmarking a go routine is transfering money back and forth
	quit := make(chan bool)
	go func() {
		for {
			if <- quit {
				break
			}
			RunInTransaction(
				db,
				sql.LevelReadUncommitted,
				func(db DbInterfacer, isLogging bool) {
					Transfer(db, isLogging)
					TransferBack(db, isLogging)
				},
				false,
			)
		}
	}()
	for i := 0; i < b.N; i++ {
		for i := 0; i < 1000; i++ {
			RunInTransaction(db, sql.LevelReadUncommitted, CheckAccounts, false)
		}
	}

	quit <- true // need to break when test is done so otherwise earlier 
	// transactions always seem like they're faster no matter that transaction level
}

