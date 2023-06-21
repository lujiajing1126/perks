// Package quantile computes approximate quantiles over an unbounded data
// stream within low memory and CPU bounds.
//
// A small amount of accuracy is traded to achieve the above properties.
//
// Multiple streams can be merged before calling Query to generate a single set
// of results. This is meaningful when the streams represent the same type of
// data. See Merge and Samples.
//
// For more detailed information about the algorithm used, see:
//
// # Effective Computation of Biased Quantiles over Data Streams
//
// http://www.cs.rutgers.edu/~muthu/bquant.pdf
package quantile

import (
	"container/list"
	"math"
	"sort"
)

// Sample holds an observed value and meta information for compression. JSON
// tags have been added for convenience.
type Sample struct {
	Value float64 `json:",string"`
	Width float64 `json:",string"`
	Delta float64 `json:",string"`
}

// Samples represents a slice of samples. It implements sort.Interface.
type Samples []Sample

func (a Samples) Len() int           { return len(a) }
func (a Samples) Less(i, j int) bool { return a[i].Value < a[j].Value }
func (a Samples) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

type invariant func(s *stream, r float64) float64

// NewLowBiased returns an initialized Stream for low-biased quantiles
// (e.g. 0.01, 0.1, 0.5) where the needed quantiles are not known a priori, but
// error guarantees can still be given even for the lower ranks of the data
// distribution.
//
// The provided epsilon is a relative error, i.e. the true quantile of a value
// returned by a query is guaranteed to be within (1±Epsilon)*Quantile.
//
// See http://www.cs.rutgers.edu/~muthu/bquant.pdf for time, space, and error
// properties.
func NewLowBiased(epsilon float64) *Stream {
	ƒ := func(s *stream, r float64) float64 {
		return 2 * epsilon * r
	}
	return newStream(ƒ)
}

// NewHighBiased returns an initialized Stream for high-biased quantiles
// (e.g. 0.01, 0.1, 0.5) where the needed quantiles are not known a priori, but
// error guarantees can still be given even for the higher ranks of the data
// distribution.
//
// The provided epsilon is a relative error, i.e. the true quantile of a value
// returned by a query is guaranteed to be within 1-(1±Epsilon)*(1-Quantile).
//
// See http://www.cs.rutgers.edu/~muthu/bquant.pdf for time, space, and error
// properties.
func NewHighBiased(epsilon float64) *Stream {
	ƒ := func(s *stream, r float64) float64 {
		return 2 * epsilon * (s.n - r)
	}
	return newStream(ƒ)
}

// NewTargeted returns an initialized Stream concerned with a particular set of
// quantile values that are supplied a priori. Knowing these a priori reduces
// space and computation time. The targets map maps the desired quantiles to
// their absolute errors, i.e. the true quantile of a value returned by a query
// is guaranteed to be within (Quantile±Epsilon).
//
// See http://www.cs.rutgers.edu/~muthu/bquant.pdf for time, space, and error properties.
func NewTargeted(targetMap map[float64]float64) *Stream {
	// Convert map to slice to avoid slow iterations on a map.
	// ƒ is called on the hot path, so converting the map to a slice
	// beforehand results in significant CPU savings.
	targets := targetMapToSlice(targetMap)

	ƒ := func(s *stream, r float64) float64 {
		var m = math.MaxFloat64
		var f float64
		for _, t := range targets {
			if t.quantile*s.n <= r {
				f = (2 * t.epsilon * r) / t.quantile
			} else {
				f = (2 * t.epsilon * (s.n - r)) / (1 - t.quantile)
			}
			if f < m {
				m = f
			}
		}
		return m
	}
	return newStream(ƒ)
}

type target struct {
	quantile float64
	epsilon  float64
}

func targetMapToSlice(targetMap map[float64]float64) []target {
	targets := make([]target, 0, len(targetMap))

	for quantile, epsilon := range targetMap {
		t := target{
			quantile: quantile,
			epsilon:  epsilon,
		}
		targets = append(targets, t)
	}

	return targets
}

// Stream computes quantiles for a stream of float64s. It is not thread-safe by
// design. Take care when using across multiple goroutines.
type Stream struct {
	*stream
	b      Samples
	sorted bool
}

func newStream(ƒ invariant) *Stream {
	x := &stream{ƒ: ƒ, l: list.New()}
	return &Stream{x, make(Samples, 0, 500), true}
}

// Insert inserts v into the stream.
func (s *Stream) Insert(v float64) {
	s.insert(Sample{Value: v, Width: 1})
}

func (s *Stream) insert(sample Sample) {
	s.b = append(s.b, sample)
	s.sorted = false
	if len(s.b) == cap(s.b) {
		s.flush()
	}
}

// Query returns the computed qth percentiles value. If s was created with
// NewTargeted, and q is not in the set of quantiles provided a priori, Query
// will return an unspecified result.
func (s *Stream) Query(q float64) float64 {
	if !s.flushed() {
		// Fast path when there hasn't been enough data for a flush;
		// this also yields better accuracy for small sets of data.
		l := len(s.b)
		if l == 0 {
			return 0
		}
		i := int(math.Ceil(float64(l) * q))
		if i > 0 {
			i -= 1
		}
		s.maybeSort()
		return s.b[i].Value
	}
	s.flush()
	return s.stream.query(q)
}

// Merge merges samples into the underlying streams samples. This is handy when
// merging multiple streams from separate threads, database shards, etc.
//
// ATTENTION: This method is broken and does not yield correct results. The
// underlying algorithm is not capable of merging streams correctly.
func (s *Stream) Merge(samples Samples) {
	sort.Sort(samples)
	s.stream.merge(samples)
}

// Reset reinitializes and clears the list reusing the samples buffer memory.
func (s *Stream) Reset() {
	s.stream.reset()
	s.b = s.b[:0]
}

// Samples returns stream samples held by s.
func (s *Stream) Samples() Samples {
	if !s.flushed() {
		return s.b
	}
	s.flush()
	return s.stream.samples()
}

// Count returns the total number of samples observed in the stream
// since initialization.
func (s *Stream) Count() int {
	return len(s.b) + s.stream.count()
}

func (s *Stream) flush() {
	s.maybeSort()
	s.stream.merge(s.b)
	s.b = s.b[:0]
}

func (s *Stream) maybeSort() {
	if !s.sorted {
		s.sorted = true
		sort.Sort(s.b)
	}
}

func (s *Stream) flushed() bool {
	return s.stream.l.Len() > 0
}

type stream struct {
	n float64
	l *list.List
	ƒ invariant
}

func (s *stream) reset() {
	s.l = s.l.Init()
	s.n = 0
}

func (s *stream) insert(v float64) {
	s.merge(Samples{{v, 1, 0}})
}

func (s *stream) merge(samples Samples) {
	// TODO(beorn7): This tries to merge not only individual samples, but
	// whole summaries. The paper doesn't mention merging summaries at
	// all. Unittests show that the merging is inaccurate. Find out how to
	// do merges properly.
	var r float64
	var e = s.l.Front()
	for _, sample := range samples {
		for ; e != nil; e = e.Next() {
			c := e.Value.(Sample)
			if c.Value > sample.Value {
				// Insert at position i.
				sample.Delta = math.Max(sample.Delta, math.Floor(s.ƒ(s, r))-1)
				e = s.l.InsertBefore(sample, e)
				goto inserted
			}
			r += c.Width
		}
		e = s.l.PushBack(Sample{sample.Value, sample.Width, 0})
	inserted:
		s.n += sample.Width
		r += sample.Width
	}
	s.compress()
}

func (s *stream) count() int {
	return int(s.n)
}

func (s *stream) query(q float64) float64 {
	t := math.Ceil(q * s.n)
	t += math.Ceil(s.ƒ(s, t) / 2)
	p := s.l.Front()
	var r float64
	for e := p.Next(); e != nil; e = e.Next() {
		r += p.Value.(Sample).Width
		if r+e.Value.(Sample).Width+e.Value.(Sample).Delta > t {
			return p.Value.(Sample).Value
		}
		p = e
	}
	return p.Value.(Sample).Value
}

func (s *stream) compress() {
	if s.l.Len() < 2 {
		return
	}
	x := s.l.Back()
	r := s.n - 1 - x.Value.(Sample).Width

	for e := x.Prev(); e != nil; {
		c := e.Value.(Sample)
		if c.Width+x.Value.(Sample).Width+x.Value.(Sample).Delta <= s.ƒ(s, r) { // merge
			x.Value = Sample{
				Value: x.Value.(Sample).Value,
				Width: x.Value.(Sample).Width + c.Width,
				Delta: x.Value.(Sample).Delta,
			}
			// Remove element at i.
			prev := e.Prev()
			s.l.Remove(e)
			e = prev
		} else {
			x = e
			e = e.Prev()
		}
		r -= c.Width
	}
}

func (s *stream) samples() Samples {
	samples := make(Samples, 0, s.l.Len())
	for e := s.l.Front(); e != nil; e = e.Next() {
		samples = append(samples, e.Value.(Sample))
	}
	return samples
}
