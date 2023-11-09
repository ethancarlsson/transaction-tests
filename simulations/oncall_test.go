package simulations

import (
	"database/sql"
	"testing"
)

func BenchmarkReadCommittedOnCall(b *testing.B) {
	GetDB()
	for i := 0; i < b.N; i++ {
		_, _ = OnCall(sql.LevelReadCommitted, false)
	}
}

func BenchmarkSerializabilityOnCall(b *testing.B) {
	GetDB()
	for i := 0; i < b.N; i++ {
		_, _ = OnCall(sql.LevelSerializable, false)
	}
}

func BenchmarkNoTOnCall(b *testing.B) {
	GetDB()
	for i := 0; i < b.N; i++ {
		_, _ = NoTOnCall(false)
	}
}

