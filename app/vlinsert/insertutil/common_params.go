package insertutil

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httputil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/timeutil"
	"github.com/VictoriaMetrics/metrics"
	"github.com/valyala/fastrand"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// CommonParams contains common HTTP parameters used by log ingestion APIs.
//
// See https://docs.victoriametrics.com/victorialogs/data-ingestion/#http-parameters
type CommonParams struct {
	TenantID         logstorage.TenantID
	TimeFields       []string
	MsgFields        []string
	StreamFields     []string
	IgnoreFields     []string
	DecolorizeFields []string
	PreserveJSONKeys []string
	ExtraFields      []logstorage.Field

	// IsTimeFieldSet means whether the TimeFields is set **manually**.
	// The TimeFields has default value `_time`. It's not empty even if the IsTimeFieldSet is false.
	IsTimeFieldSet bool

	Debug           bool
	DebugRequestURI string
	DebugRemoteAddr string
}

// GetCommonParams returns CommonParams from r.
func GetCommonParams(r *http.Request) (*CommonParams, error) {
	// Extract tenantID
	tenantID, err := logstorage.GetTenantIDFromRequest(r)
	if err != nil {
		return nil, err
	}

	var isTimeFieldSet bool
	timeFields := []string{"_time"}
	if tfs := getArray(r, "_time_field", "VL-Time-Field"); len(tfs) > 0 {
		isTimeFieldSet = true
		timeFields = tfs
	}

	msgFields := getArray(r, "_msg_field", "VL-Msg-Field")
	streamFields := getArray(r, "_stream_fields", "VL-Stream-Fields")
	ignoreFields := getArray(r, "ignore_fields", "VL-Ignore-Fields")
	decolorizeFields := getArray(r, "decolorize_fields", "VL-Decolorize-Fields")
	preserveJSONKeys := getArray(r, "preserve_json_keys", "VL-Preserve-JSON-Keys")

	// verify that the _stream_fields contains valid values
	if err := logstorage.CheckStreamFieldNames(streamFields); err != nil {
		return nil, fmt.Errorf("cannot parse stream field names from the _stream_fields query arg or from VL-Stream-Fields header: %w", err)
	}

	extraFields, err := getExtraFields(r)
	if err != nil {
		return nil, err
	}

	debug := false
	if dv := httputil.GetRequestValue(r, "debug", "VL-Debug"); dv != "" {
		debug, err = strconv.ParseBool(dv)
		if err != nil {
			return nil, fmt.Errorf("cannot parse debug=%q: %w", dv, err)
		}
	}
	debugRequestURI := ""
	debugRemoteAddr := ""
	if debug {
		debugRequestURI = httpserver.GetRequestURI(r)
		debugRemoteAddr = httpserver.GetQuotedRemoteAddr(r)
	}

	cp := &CommonParams{
		TenantID:         tenantID,
		TimeFields:       timeFields,
		MsgFields:        msgFields,
		StreamFields:     streamFields,
		IgnoreFields:     ignoreFields,
		DecolorizeFields: decolorizeFields,
		PreserveJSONKeys: preserveJSONKeys,
		ExtraFields:      extraFields,

		IsTimeFieldSet:  isTimeFieldSet,
		Debug:           debug,
		DebugRequestURI: debugRequestURI,
		DebugRemoteAddr: debugRemoteAddr,
	}

	return cp, nil
}

func getExtraFields(r *http.Request) ([]logstorage.Field, error) {
	efs := getArray(r, "extra_fields", "VL-Extra-Fields")
	if len(efs) == 0 {
		return nil, nil
	}

	extraFields := make([]logstorage.Field, len(efs))
	for i, ef := range efs {
		n := strings.Index(ef, "=")
		if n <= 0 || n == len(ef)-1 {
			return nil, fmt.Errorf(`invalid extra_field format: %q; must be in the form "field=value"`, ef)
		}
		extraFields[i] = logstorage.Field{
			Name:  ef[:n],
			Value: ef[n+1:],
		}
	}
	return extraFields, nil
}

func getArray(r *http.Request, argKey, headerKey string) []string {
	a := httputil.GetArray(r, argKey, headerKey)
	return removeEmptyTokens(a)
}

func removeEmptyTokens(a []string) []string {
	dst := a[:0]
	for _, s := range a {
		s = strings.TrimSpace(s)
		if s != "" {
			dst = append(dst, s)
		}
	}
	return dst
}

//USECASE- Unique ID Generation - Authored by NiyatiSinha-yb
// batchIDSeq and processSeed back newBatchID. processSeed is randomized once
// per process start so that two VictoriaLogs processes generating a batchID
// at the same nanosecond with the same sequence value is not enough to
// collide — all three components would have to match.
var (
        batchIDSeq  atomic.Uint64
        processSeed = fastrand.Uint32()
)
// newBatchID returns a value unique for the lifetime of this process,
// identifying every row processed by one logMessageProcessor instance (one
// per ingest HTTP request). Deliberately dependency-free — this module has
// no UUID library in go.mod, and this codebase already prefers hand-rolled
// primitives (fastrand, fasttime) over adding third-party equivalents.
func newBatchID() string {
        seq := batchIDSeq.Add(1)
        return strconv.FormatUint(uint64(processSeed), 36) + "-" +
                strconv.FormatInt(time.Now().UnixNano(), 36) + "-" +
                strconv.FormatUint(seq, 36)
}
// setField overwrites the value of an existing field named `name` in
// fields, or appends a new field if none exists. Used for the
// system-assigned _batch_id/_row_offset fields so that a client JSON
// payload which happens to already use one of those keys gets overwritten
// rather than producing two fields with the same Name in one row.
func setField(fields []logstorage.Field, name, value string) []logstorage.Field {
        for i := range fields {
                if fields[i].Name == name {
                        fields[i].Value = value
                        return fields
                }
        }
        return append(fields, logstorage.Field{Name: name, Value: value})
}

// GetCommonParamsForSyslog returns common params needed for parsing syslog messages and storing them to the given tenantID.
func GetCommonParamsForSyslog(tenantID logstorage.TenantID, streamFields, ignoreFields, decolorizeFields []string, extraFields []logstorage.Field) *CommonParams {
	// See https://docs.victoriametrics.com/victorialogs/logsql/#unpack_syslog-pipe
	if streamFields == nil {
		streamFields = []string{
			"hostname",
			"app_name",
			"proc_id",
			"cef.device_vendor",
			"cef.device_product",
			"cef.device_event_class_id",
		}
	}
	cp := &CommonParams{
		TenantID: tenantID,
		TimeFields: []string{
			"timestamp",
		},
		MsgFields: []string{
			"message",
		},
		StreamFields:     streamFields,
		IgnoreFields:     ignoreFields,
		DecolorizeFields: decolorizeFields,
		ExtraFields:      extraFields,
	}

	return cp
}

// LogRowsStorage is an interface for ingesting logs into the storage.
type LogRowsStorage interface {
	// MustAddRows must add lr to the underlying storage.
	MustAddRows(lr *logstorage.LogRows)

	// CanWriteData must returns non-nil error if logs cannot be added to the underlying storage.
	CanWriteData() error
}

var logRowsStorage LogRowsStorage

// SetLogRowsStorage sets the storage for writing data to via LogMessageProcessor.
//
// This function must be called before using LogMessageProcessor and CanWriteData from this package.
func SetLogRowsStorage(storage LogRowsStorage) {
	logRowsStorage = storage
}

// CanWriteData returns non-nil error if data cannot be written to the underlying storage.
func CanWriteData() error {
	return logRowsStorage.CanWriteData()
}

// LogMessageProcessor is an interface for log message processors.
type LogMessageProcessor interface {
	// AddRow must add row to the LogMessageProcessor with the given timestamp and fields.
	//
	// If streamFieldsLen >= 0, then the given number of initial fields must be used as log stream fields instead of pre-configured fields.
	//
	// The LogMessageProcessor implementation cannot hold references to fields, since the caller can reuse them.
	AddRow(timestamp int64, fields []logstorage.Field, streamFieldsLen int)

	// MustClose() must flush all the remaining fields and free up resources occupied by LogMessageProcessor.
	MustClose()
}

type logMessageProcessor struct {
	mu            sync.Mutex
	wg            sync.WaitGroup
	stopCh        chan struct{}
	lastFlushTime time.Time

	cp *CommonParams
	lr *logstorage.LogRows

	//USECASE- batch id and rowOffset defined in struct for Unique ID Generation - Authored by NiyatiSinha-yb
	// batchID is constant for every row this processor ever handles — one
	// logMessageProcessor is created per ingest HTTP request (see
	// NewLogMessageProcessor), so this is effectively "one UUID per
	// request." rowOffset is the position of a row within that batch,
	// assigned only to rows confirmed to actually be stored — see AddRow.
	batchID   string
	rowOffset uint64

	rowsIngestedTotal  *metrics.Counter
	bytesIngestedTotal *metrics.Counter
	flushDuration      *metrics.Summary

	unflushedRows  int
	unflushedBytes int
}

func (lmp *logMessageProcessor) initPeriodicFlush() {
	lmp.lastFlushTime = time.Now()

	lmp.wg.Go(func() {
		d := timeutil.AddJitterToDuration(time.Second)
		ticker := time.NewTicker(d)
		defer ticker.Stop()

		for {
			select {
			case <-lmp.stopCh:
				return
			case <-ticker.C:
				lmp.mu.Lock()
				if time.Since(lmp.lastFlushTime) >= d {
					lmp.flushLocked()
				}
				lmp.mu.Unlock()
			}
		}
	})
}

// AddRow adds new log message to lmp with the given timestamp and fields.
//
// If streamFieldsLen >= 0, then the given number of the initial fields is used as log stream fields
// instead of the pre-configured stream fields.
func (lmp *logMessageProcessor) AddRow(timestamp int64, fields []logstorage.Field, streamFieldsLen int) {
	lmp.mu.Lock()
	defer lmp.mu.Unlock()

	//USECASE- set a new batch id and row offset - Authored by NiyatiSinha-yb
	if *AddRecordIdentity {
			fields = setField(fields, "_batch_id", lmp.batchID)
			fields = setField(fields, "_row_offset", strconv.FormatUint(lmp.rowOffset, 10))
	}

	lmp.unflushedRows++
	n := logstorage.EstimatedJSONRowLen(fields)
	lmp.unflushedBytes += n

	if len(fields) > *MaxFieldsPerLine {
		line := logstorage.MarshalFieldsToJSON(nil, fields)
		logger.Warnf("dropping log line with %d fields; it exceeds -insert.maxFieldsPerLine=%d; %s", len(fields), *MaxFieldsPerLine, line)
		rowsDroppedTotalTooManyFields.Inc()
		return
	}

	//USECASE- update rowoffset counter - Authored by NiyatiSinha-yb
	if *AddRecordIdentity {
                // Only advance the counter once the row is confirmed to be stored
                // (past the field-count check above), so _row_offset has no gaps
                // relative to what's actually queryable.
                lmp.rowOffset++
    }

	lmp.lr.MustAdd(lmp.cp.TenantID, timestamp, fields, streamFieldsLen)

	if lmp.cp.Debug {
		s := lmp.lr.GetRowString(0)
		lmp.lr.ResetKeepSettings()
		logger.Infof("remoteAddr=%s; requestURI=%s; ignoring log entry because of `debug` arg: %s", lmp.cp.DebugRemoteAddr, lmp.cp.DebugRequestURI, s)
		rowsDroppedTotalDebug.Inc()
		return
	}
	if lmp.lr.NeedFlush() {
		lmp.flushLocked()
	}
}

// InsertRowProcessor is used by native data ingestion protocol parser.
type InsertRowProcessor interface {
	// AddInsertRow must add r to the underlying storage.
	AddInsertRow(r *logstorage.InsertRow)
}

// AddInsertRow adds r to lmp.
func (lmp *logMessageProcessor) AddInsertRow(r *logstorage.InsertRow) {
	lmp.mu.Lock()
	defer lmp.mu.Unlock()

	//USECASE- add batch_id and row_offset for the added record/row - Authored by NiyatiSinha-yb
	//Treating AddRecordIdentity as a feature flag
	if *AddRecordIdentity {
                r.Fields = setField(r.Fields, "_batch_id", lmp.batchID)
                r.Fields = setField(r.Fields, "_row_offset", strconv.FormatUint(lmp.rowOffset, 10))
        }

	lmp.unflushedRows++
	n := logstorage.EstimatedJSONRowLen(r.Fields)
	lmp.unflushedBytes += n
	

	if len(r.Fields) > *MaxFieldsPerLine {
		line := logstorage.MarshalFieldsToJSON(nil, r.Fields)
		logger.Warnf("dropping log line with %d fields; it exceeds -insert.maxFieldsPerLine=%d; %s", len(r.Fields), *MaxFieldsPerLine, line)
		rowsDroppedTotalTooManyFields.Inc()
		return
	}

	//USECASE- update rowOffset once it has been set for a row  - Authored by NiyatiSinha-yb
	//Treating AddRecordIdentity as a feature flag
	if *AddRecordIdentity {
                lmp.rowOffset++
    }

	lmp.lr.MustAddInsertRow(r)

	if lmp.cp.Debug {
		s := lmp.lr.GetRowString(0)
		lmp.lr.ResetKeepSettings()
		logger.Infof("remoteAddr=%s; requestURI=%s; ignoring log entry because of `debug` arg: %s", lmp.cp.DebugRemoteAddr, lmp.cp.DebugRequestURI, s)
		rowsDroppedTotalDebug.Inc()
		return
	}
	if lmp.lr.NeedFlush() {
		lmp.flushLocked()
	}
}

// flushLocked must be called under locked lmp.mu.
func (lmp *logMessageProcessor) flushLocked() {
	start := time.Now()
	lmp.lastFlushTime = start
	logRowsStorage.MustAddRows(lmp.lr)
	lmp.lr.ResetKeepSettings()

	lmp.flushDuration.UpdateDuration(start)
	lmp.rowsIngestedTotal.Add(lmp.unflushedRows)
	lmp.bytesIngestedTotal.Add(lmp.unflushedBytes)

	lmp.unflushedRows = 0
	lmp.unflushedBytes = 0
}

// MustClose flushes the remaining data to the underlying storage and closes lmp.
func (lmp *logMessageProcessor) MustClose() {
	close(lmp.stopCh)
	lmp.wg.Wait()

	lmp.flushLocked()
	logstorage.PutLogRows(lmp.lr)
	lmp.lr = nil
	messageProcessorCount.Add(-1)
}

// NewLogMessageProcessor returns new LogMessageProcessor for the given cp.
//
// MustClose() must be called on the returned LogMessageProcessor when it is no longer needed.
func (cp *CommonParams) NewLogMessageProcessor(protocolName string, isStreamMode bool) LogMessageProcessor {
	lr := logstorage.GetLogRows(cp.StreamFields, cp.IgnoreFields, cp.DecolorizeFields, cp.ExtraFields, *DefaultMsgValue)

	rowsIngestedTotal := metrics.GetOrCreateCounter(fmt.Sprintf("vl_rows_ingested_total{type=%q}", protocolName))
	bytesIngestedTotal := metrics.GetOrCreateCounter(fmt.Sprintf("vl_bytes_ingested_total{type=%q}", protocolName))
	flushDuration := metrics.GetOrCreateSummary(fmt.Sprintf("vl_insert_flush_duration_seconds{type=%q}", protocolName))

	//USECASE- update logMessageProcessor to include batchID  - Authored by NiyatiSinha-yb
	lmp := &logMessageProcessor{
		cp: cp,
		lr: lr,

		batchID: newBatchID(),
		rowsIngestedTotal:  rowsIngestedTotal,
		bytesIngestedTotal: bytesIngestedTotal,
		flushDuration:      flushDuration,

		stopCh: make(chan struct{}),
	}

	if isStreamMode {
		lmp.initPeriodicFlush()
	}

	messageProcessorCount.Add(1)
	return lmp
}

var (
	rowsDroppedTotalDebug         = metrics.NewCounter(`vl_rows_dropped_total{reason="debug"}`)
	rowsDroppedTotalTooManyFields = metrics.GetOrCreateCounter(`vl_rows_dropped_total{reason="too_many_fields"}`)
	_                             = metrics.NewGauge(`vl_insert_processors_count`, func() float64 { return float64(messageProcessorCount.Load()) })
	messageProcessorCount         atomic.Int64
)

// IsJSONContentType returns true if ct is JSON content-type.
func IsJSONContentType(ct string) bool {
	return ct == "application/json" || strings.HasPrefix(ct, "application/json;")
}
