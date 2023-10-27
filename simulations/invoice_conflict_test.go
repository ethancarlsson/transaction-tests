package simulations

import (
	"database/sql"
	"testing"
)

func BenchmarkSerializableInvoiceConflict(b *testing.B) {
	GetDB() // Get db ahead of time to cache it
	for i := 0; i < b.N; i++ {
		for i := 0; i < 1000; i++ {
			_, _ = InvoiceConflict(sql.LevelSerializable, false)
		}
	}
}

func BenchmarkReadCommittedInvoiceConflict(b *testing.B) {
	GetDB() // Get db ahead of time to cache it
	for i := 0; i < b.N; i++ {
		for i := 0; i < 1000; i++ {
			_, _ = InvoiceConflict(sql.LevelReadCommitted, false)
		}
	}
}

func BenchmarkReadUncommittedInvoiceConflict(b *testing.B) {
	GetDB() // Get db ahead of time to cache it
	for i := 0; i < b.N; i++ {
		for i := 0; i < 1000; i++ {
			_, _ = InvoiceConflict(sql.LevelReadUncommitted, false)
		}
	}
}

func BenchmarkNoTransactionInvoiceConflict(b *testing.B) {
	GetDB() // Get db ahead of time to cache it
	for i := 0; i < b.N; i++ {
		for i := 0; i < 1000; i++ {
			_, _ = InvoiceConflictNoTrans(false)
		}
	}
}
