package simulations

import (
	"database/sql"
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

