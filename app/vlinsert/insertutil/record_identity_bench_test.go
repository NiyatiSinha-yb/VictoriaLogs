/*
To get a comparision between the performance of the record identity feature, run:
cd VictoriaMetrics/VictoriaLogs/app/vlinsert 

# 1. Run the benchmark (targeting current directory .)
go test -run=^$ -bench=. -benchmem -count=10 . > full_bench.txt

# 2. Extract baseline (off) and feature (on) runs into separate files
grep "recordIdentity=off" full_bench.txt > off.txt
grep "recordIdentity=on" full_bench.txt > on.txt

# 3. Trim the sub-benchmark names so benchstat can pair them up
sed -i '' 's/\/recordIdentity=off//' off.txt
sed -i '' 's/\/recordIdentity=on//' on.txt

# 4. Compare the results

benchstat off.txt on.txt

#or

grep "recordIdentity=off" full_bench.txt | sed 's/\/recordIdentity=off//' > off.txt
grep "recordIdentity=on" full_bench.txt | sed 's/\/recordIdentity=on//' > on.txt
$(go env GOPATH)/bin/benchstat off.txt on.txt

*/

package insertutil

import (
	"strconv"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

/*
Storage Mocking: init() runs automatically before any test. 
It swaps out production disk storage with an in-memory mock (BenchmarkStorage{}) so file I/O doesn't obscure CPU and memory performance.
*/
func init() {
	SetLogRowsStorage(BenchmarkStorage{})
}


/*
Feature Flag Isolation: Safely toggles *AddRecordIdentity to true or false for a specific benchmark run, 
using b.Cleanup to restore the original flag state once the test completes.
*/
func withAddRecordIdentity(b *testing.B, v bool) {
	b.Helper()
	prev := *AddRecordIdentity
	*AddRecordIdentity = v
	b.Cleanup(func() { *AddRecordIdentity = prev })
}

/*
Realistic Slice Pre-allocation: Constructs n dummy fields (field_0: value_0, field_1: value_1, ...). 
Allocating capacity for n+2 fields (make([], n, n+2)) mimics real production log parsers, which carry spare slice capacity. 
This prevents Go array reallocations from distorting the benchmarks.
*/
func makeFields(n int) []logstorage.Field {
	// Pre-allocate capacity for 2 extra slots (_batch_id, _row_offset)
	// to mirror real production parser slack capacity.
	fields := make([]logstorage.Field, n, n+2)
	for i := 0; i < n; i++ {
		fields[i] = logstorage.Field{
			Name:  "field_" + strconv.Itoa(i),
			Value: "value_" + strconv.Itoa(i),
		}
	}
	return fields
}

func BenchmarkNewBatchID(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = newBatchID()
	}
}

func BenchmarkNewBatchID_Parallel(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = newBatchID()
		}
	})
}

func BenchmarkSetField_Append(b *testing.B) {
	b.ReportAllocs()
	base := makeFields(20)
	for i := 0; i < b.N; i++ {
		fields := append([]logstorage.Field(nil), base...)
		fields = setField(fields, "_batch_id", "a3f-19c2d4e5f6a7-142")
		fields = setField(fields, "_row_offset", "42")
	}
}

func BenchmarkSetField_Overwrite(b *testing.B) {
	b.ReportAllocs()
	base := makeFields(18)
	base = append(base,
		logstorage.Field{Name: "_batch_id", Value: "client-supplied"},
		logstorage.Field{Name: "_row_offset", Value: "0"},
	)
	for i := 0; i < b.N; i++ {
		fields := append([]logstorage.Field(nil), base...)
		fields = setField(fields, "_batch_id", "a3f-19c2d4e5f6a7-142")
		fields = setField(fields, "_row_offset", "42")
	}
}

func BenchmarkAddRow_RecordIdentity(b *testing.B) {
	for _, n := range []int{5, 20, 100} {
		n := n
		b.Run("fields="+strconv.Itoa(n)+"/recordIdentity=on", func(b *testing.B) {
			withAddRecordIdentity(b, true)
			benchmarkAddRow_RecordIdentity(b, n)
		})
		b.Run("fields="+strconv.Itoa(n)+"/recordIdentity=off", func(b *testing.B) {
			withAddRecordIdentity(b, false)
			benchmarkAddRow_RecordIdentity(b, n)
		})
	}
}

func benchmarkAddRow_RecordIdentity(b *testing.B, numFields int) {
	b.Helper()
	cp := &CommonParams{TimeFields: []string{"_time"}}
	lmp := cp.NewLogMessageProcessor("benchmark", false)
	defer lmp.MustClose()

	fields := makeFields(numFields)
	ts := int64(1_700_000_000_000_000_000)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row := make([]logstorage.Field, len(fields), cap(fields))
		copy(row, fields)
		lmp.AddRow(ts, row, -1)
	}
}

func BenchmarkAddInsertRow(b *testing.B) {
	for _, n := range []int{5, 20, 100} {
		n := n
		b.Run("fields="+strconv.Itoa(n)+"/recordIdentity=on", func(b *testing.B) {
			withAddRecordIdentity(b, true)
			benchmarkAddInsertRow(b, n)
		})
		b.Run("fields="+strconv.Itoa(n)+"/recordIdentity=off", func(b *testing.B) {
			withAddRecordIdentity(b, false)
			benchmarkAddInsertRow(b, n)
		})
	}
}

func benchmarkAddInsertRow(b *testing.B, numFields int) {
	b.Helper()
	cp := &CommonParams{TimeFields: []string{"_time"}}
	lmp := cp.NewLogMessageProcessor("benchmark-native", false)
	defer lmp.MustClose()
	irp := lmp.(InsertRowProcessor)

	baseFields := makeFields(numFields)
	ts := int64(1_700_000_000_000_000_000)

	// Valid stream tags canonical payload prevents MustAddInsertRow validation drops
	st := logstorage.GetStreamTags()
	streamTagsCanonical := string(st.MarshalCanonical(nil))
	logstorage.PutStreamTags(st)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := logstorage.GetInsertRow()
		r.Timestamp = ts
		r.StreamTagsCanonical = streamTagsCanonical
		r.Fields = append(r.Fields[:0], baseFields...)
		irp.AddInsertRow(r)
		logstorage.PutInsertRow(r)
	}
}

func BenchmarkNewLogMessageProcessor(b *testing.B) {
	b.Run("recordIdentity=on", func(b *testing.B) {
		withAddRecordIdentity(b, true)
		benchmarkNewLMP(b)
	})
	b.Run("recordIdentity=off", func(b *testing.B) {
		withAddRecordIdentity(b, false)
		benchmarkNewLMP(b)
	})
}

func benchmarkNewLMP(b *testing.B) {
	b.Helper()
	cp := &CommonParams{TimeFields: []string{"_time"}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lmp := cp.NewLogMessageProcessor("benchmark", false)
		lmp.MustClose()
	}
}