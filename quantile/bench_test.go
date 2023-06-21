package quantile

import (
	"math/rand"
	"testing"
)

var TargetsZeroEpsilon = map[float64]float64{
	0.01: 0.0,
	0.10: 0.0,
	0.50: 0.0,
	0.90: 0.0,
	0.99: 0.0,
}

func BenchmarkInsertTargeted(b *testing.B) {
	b.ReportAllocs()

	s := NewTargeted(Targets)
	b.ResetTimer()
	for i := float64(0); i < float64(b.N); i++ {
		s.Insert(i)
	}
}

func BenchmarkInsertTargetedSmallEpsilon(b *testing.B) {
	b.ReportAllocs()
	s := NewTargeted(TargetsSmallEpsilon)
	nums := make([]float64, 0, b.N)
	for i := 0; i < b.N; i++ {
		nums = append(nums, rand.Float64()*10)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Insert(nums[i])
	}
}

func BenchmarkInsertBiased(b *testing.B) {
	s := NewLowBiased(0.01)
	b.ResetTimer()
	for i := float64(0); i < float64(b.N); i++ {
		s.Insert(i)
	}
}

func BenchmarkInsertBiasedSmallEpsilon(b *testing.B) {
	s := NewLowBiased(0.0001)
	b.ResetTimer()
	for i := float64(0); i < float64(b.N); i++ {
		s.Insert(i)
	}
}

func BenchmarkQuery(b *testing.B) {
	s := NewTargeted(Targets)
	for i := float64(0); i < 1e6; i++ {
		s.Insert(i)
	}
	b.ResetTimer()
	n := float64(b.N)
	for i := float64(0); i < n; i++ {
		s.Query(i / n)
	}
}

func BenchmarkQuerySmallEpsilon(b *testing.B) {
	s := NewTargeted(TargetsSmallEpsilon)
	for i := float64(0); i < 1e6; i++ {
		s.Insert(i)
	}
	b.ResetTimer()
	n := float64(b.N)
	for i := float64(0); i < n; i++ {
		s.Query(i / n)
	}
}
