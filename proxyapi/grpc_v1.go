package proxyapi

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"time"

	"go.opencensus.io/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/pkg/seqproxyapi/v1"
	"github.com/ozontech/seq-db/proxy/search"
	"github.com/ozontech/seq-db/querytracer"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

type SearchIngestor interface {
	Search(ctx context.Context, sr *search.SearchRequest, tr *querytracer.Tracer) (*seq.QPR, search.DocsIterator, time.Duration, error)
	Documents(ctx context.Context, r search.FetchRequest) (search.DocsIterator, error)
	Status(ctx context.Context) *search.IngestorStatus
	StartAsyncSearch(context.Context, search.AsyncRequest) (search.AsyncResponse, error)
	FetchAsyncSearchResult(context.Context, search.FetchAsyncSearchResultRequest) (search.FetchAsyncSearchResultResponse, search.DocsIterator, error)
	CancelAsyncSearch(ctx context.Context, id string) error
	DeleteAsyncSearch(ctx context.Context, id string) error
	GetAsyncSearchesList(context.Context, search.GetAsyncSearchesListRequest) ([]*search.AsyncSearchesListItem, error)
}

type MappingProvider interface {
	GetRawMapping() *seq.RawMapping
}

type RateLimiter interface {
	Account(string) bool
}

// To generate mocks

type ExportServer interface {
	seqproxyapi.SeqProxyApi_ExportServer
}

type FetchServer interface {
	seqproxyapi.SeqProxyApi_FetchServer
}

type grpcV1 struct {
	seqproxyapi.UnimplementedSeqProxyApiServer

	config          APIConfig
	searchIngestor  SearchIngestor
	mappingProvider MappingProvider
	rateLimiter     RateLimiter
	mirror          seqproxyapi.SeqProxyApiClient
	mirrorRequests  atomic.Int64
}

func newGrpcV1(
	config APIConfig,
	si SearchIngestor,
	mappingProvider MappingProvider,
	rl RateLimiter,
	mirror seqproxyapi.SeqProxyApiClient,
) *grpcV1 {
	return &grpcV1{
		config:          config,
		searchIngestor:  si,
		mappingProvider: mappingProvider,
		rateLimiter:     rl,
		mirror:          mirror,
		mirrorRequests:  atomic.Int64{},
	}
}

func processSearchErrors(qpr *seq.QPR, err error) error {
	if err == nil && len(qpr.Errors) > 0 {
		return status.Error(codes.Internal, qpr.CombineErrors())
	}
	switch {
	case errors.Is(err, consts.ErrIngestorQueryWantsOldData):
		return status.Error(codes.InvalidArgument, err.Error())
	case err != nil:
		return status.Error(codes.Internal, err.Error())
	default:
		return nil
	}
}

func makeProtoDocs(qpr *seq.QPR, docs search.DocsIterator) []*seqproxyapi.Document {
	docsBuf := make([]seqproxyapi.Document, len(qpr.IDs))
	respDocs := make([]*seqproxyapi.Document, len(qpr.IDs))
	for i, id := range qpr.IDs {
		doc := &docsBuf[i]
		doc.Id = id.ID.String()
		doc.Time = timestamppb.New(id.ID.MID.Time())
		d, _ := docs.Next()
		doc.Data = d.Data
		respDocs[i] = doc
	}
	return respDocs
}

func makeProtoHistogram(qpr *seq.QPR) *seqproxyapi.Histogram {
	bucketsBuf := make([]seqproxyapi.Histogram_Bucket, len(qpr.Histogram))
	buckets := make([]*seqproxyapi.Histogram_Bucket, len(qpr.Histogram))
	i := 0
	for ts, dc := range qpr.Histogram {
		bucket := &bucketsBuf[i]
		bucket.DocCount = dc
		bucket.Ts = timestamppb.New(seq.MIDToTime(ts))
		buckets[i] = bucket
		i++
	}
	return &seqproxyapi.Histogram{Buckets: buckets}
}

func makeProtoAggregation(allAggregations []seq.AggregationResult) []*seqproxyapi.Aggregation {
	aggs := make([]*seqproxyapi.Aggregation, 0, len(allAggregations))

	for _, agg := range allAggregations {
		bucketsBuf := make([]seqproxyapi.Aggregation_Bucket, len(agg.Buckets))
		buckets := make([]*seqproxyapi.Aggregation_Bucket, len(agg.Buckets))

		for i, item := range agg.Buckets {
			bucket := &bucketsBuf[i]

			bucket.Key = item.Name
			bucket.Value = item.Value

			bucket.NotExists = item.NotExists
			bucket.Quantiles = item.Quantiles

			if item.MID != consts.DummyMID {
				bucket.Ts = timestamppb.New(item.MID.Time())
			}

			buckets[i] = bucket
		}

		aggs = append(aggs, &seqproxyapi.Aggregation{
			Buckets:   buckets,
			NotExists: agg.NotExists,
		})
	}

	return aggs
}

func getSearchQueryFromGRPCReqForRateLimiter(req *seqproxyapi.ComplexSearchRequest) string {
	rlQuery := []string{req.Query.Query}
	if req.Aggs != nil {
		for _, q := range req.Aggs {
			rlQuery = append(rlQuery, q.Field, q.GroupBy, q.Func.String())
		}
	}
	if req.Hist != nil {
		rlQuery = append(rlQuery, req.Hist.Interval)
	}
	return strings.Join(rlQuery, ",")
}

type proxySearchResponse struct {
	qpr        *seq.QPR
	docsStream search.DocsIterator
	err        *seqproxyapi.Error
}

func (g *grpcV1) doSearch(
	ctx context.Context,
	req *seqproxyapi.ComplexSearchRequest,
	shouldFetch bool,
	tr *querytracer.Tracer,
) (*proxySearchResponse, error) {
	metric.SearchOverall.Add(1)

	span := trace.FromContext(ctx)
	defer span.End()

	if req.Query == nil {
		return nil, status.Error(codes.InvalidArgument, "search query must be provided")
	}
	if req.Query.From == nil || req.Query.To == nil {
		return nil, status.Error(codes.InvalidArgument, `search query "from" and "to" fields must be provided`)
	}

	fromTime := req.Query.From.AsTime()
	toTime := req.Query.To.AsTime()
	if span.IsRecordingEvents() {
		span.AddAttributes(
			trace.StringAttribute("query", req.Query.Query),
			trace.StringAttribute("from", fromTime.UTC().Format(time.RFC3339Nano)),
			trace.StringAttribute("to", toTime.UTC().Format(time.RFC3339Nano)),
			trace.BoolAttribute("explain", req.Query.Explain),
			trace.Int64Attribute("size", req.Size),
			trace.Int64Attribute("offset", req.Offset),
			trace.BoolAttribute("with_total", req.WithTotal),
			trace.StringAttribute("order", req.Order.String()),
		)
		if req.Aggs != nil {
			aggQBytes, _ := json.Marshal(req.Aggs)
			span.AddAttributes(
				trace.StringAttribute("agg", string(aggQBytes)),
			)
		}
		if req.Hist != nil {
			span.AddAttributes(
				trace.StringAttribute("interval", req.Hist.Interval),
			)
		}
	}

	rlQuery := getSearchQueryFromGRPCReqForRateLimiter(req)
	if !g.rateLimiter.Account(rlQuery) {
		return nil, status.Error(codes.ResourceExhausted, consts.ErrRequestWasRateLimited.Error())
	}

	proxyReq := &search.SearchRequest{
		Q:           []byte(req.Query.Query),
		From:        seq.MID(fromTime.UnixMilli()),
		To:          seq.MID(toTime.UnixMilli()),
		Explain:     req.Query.Explain,
		Size:        int(req.Size),
		Offset:      int(req.Offset),
		WithTotal:   req.WithTotal,
		ShouldFetch: shouldFetch,
		Order:       req.Order.MustDocsOrder(),
	}

	if len(req.Aggs) > 0 {
		aggs, err := convertAggsQuery(req.Aggs)
		if err != nil {
			return nil, err
		}
		proxyReq.AggQ = aggs
	}

	if req.Hist != nil {
		intervalDuration, err := util.ParseDuration(req.Hist.Interval)
		if err != nil {
			return nil, status.Errorf(
				codes.InvalidArgument,
				"failed to parse 'interval': %v",
				err,
			)
		}
		proxyReq.Interval = seq.MID(intervalDuration.Milliseconds())
	}

	qpr, docsStream, _, err := g.searchIngestor.Search(ctx, proxyReq, tr)
	psr := &proxySearchResponse{
		qpr:        qpr,
		docsStream: docsStream,
	}

	if e, ok := parseProxyError(err); ok {
		psr.err = e
		return psr, nil
	}

	if errors.Is(err, consts.ErrInvalidArgument) {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if st, ok := status.FromError(err); ok {
		// could not parse a query
		if st.Code() == codes.InvalidArgument {
			return nil, err
		}
	}

	if errors.Is(err, consts.ErrPartialResponse) {
		metric.SearchPartial.Inc()
		psr.err = &seqproxyapi.Error{
			Code:    seqproxyapi.ErrorCode_ERROR_CODE_PARTIAL_RESPONSE,
			Message: err.Error(),
		}
		return psr, nil
	}
	if err = processSearchErrors(qpr, err); err != nil {
		metric.SearchErrors.Inc()
		return nil, err
	}

	g.tryMirrorRequest(req)

	return psr, nil
}

func convertAggsQuery(aggs []*seqproxyapi.AggQuery) ([]search.AggQuery, error) {
	var result []search.AggQuery
	for _, agg := range aggs {
		err := validateAgg(agg)
		if err != nil {
			return nil, err
		}
		aggFunc, err := agg.Func.ToAggFunc()
		if err != nil {
			return nil, err
		}

		aggQuery := search.AggQuery{
			Field:     agg.Field,
			GroupBy:   agg.GroupBy,
			Func:      aggFunc,
			Quantiles: agg.Quantiles,
		}

		if agg.Interval == nil {
			result = append(result, aggQuery)
			continue
		}

		interval, err := util.ParseDuration(*agg.Interval)
		if err != nil {
			return nil, status.Errorf(
				codes.InvalidArgument,
				"failed to parse 'interval': %v",
				err,
			)
		}

		aggQuery.Interval = seq.MID(interval.Milliseconds())
		result = append(result, aggQuery)
	}
	return result, nil
}

func (g *grpcV1) tryMirrorRequest(req *seqproxyapi.ComplexSearchRequest) {
	if g.mirror == nil {
		return
	}
	currentMirrorRequests := g.mirrorRequests.Add(1)

	if currentMirrorRequests >= consts.MirrorRequestLimit {
		g.mirrorRequests.Add(-1)
		return
	}

	go func() {
		defer g.mirrorRequests.Add(-1)

		_, err := g.mirror.ComplexSearch(context.Background(), req)
		if err != nil {
			logger.Error("failed to mirror complex search", zap.Error(err))
			return
		}
	}()
}

func validateAgg(agg *seqproxyapi.AggQuery) error {
	switch agg.Func {
	case seqproxyapi.AggFunc_AGG_FUNC_COUNT:
		// Expect COUNT to support legacy format in which 'field' means 'groupBy'
		if agg.GroupBy == "" && agg.Field == "" {
			return status.Error(codes.InvalidArgument, "'groupBy' or 'field' must be set")
		}

	case seqproxyapi.AggFunc_AGG_FUNC_UNIQUE:
		if agg.GroupBy == "" {
			return status.Error(codes.InvalidArgument, "'groupBy' must be set")
		}

		if agg.Interval != nil {
			return status.Error(
				codes.InvalidArgument,
				"remove 'interval' parameter: 'unique' aggregations do not support timeseries",
			)
		}

	case seqproxyapi.AggFunc_AGG_FUNC_SUM, seqproxyapi.AggFunc_AGG_FUNC_MIN,
		seqproxyapi.AggFunc_AGG_FUNC_MAX, seqproxyapi.AggFunc_AGG_FUNC_AVG:
		if agg.Field == "" {
			return status.Error(codes.InvalidArgument, "'field' must be set")
		}

	case seqproxyapi.AggFunc_AGG_FUNC_QUANTILE:
		if agg.Field == "" {
			return status.Error(codes.InvalidArgument, "'field' must be set")
		}

		if len(agg.Quantiles) == 0 {
			return status.Error(codes.InvalidArgument, "aggregation query with QUANTILE function must contain at least one quantile")
		}

		for _, q := range agg.Quantiles {
			if q < 0 || q > 1 {
				return status.Error(codes.InvalidArgument, "quantile must be between 0 and 1")
			}
		}
	}
	return nil
}

func tracerSpanToExplainEntry(span *querytracer.Span) *seqproxyapi.ExplainEntry {
	if span == nil {
		return nil
	}

	ee := &seqproxyapi.ExplainEntry{
		Message:  span.Message,
		Duration: durationpb.New(span.Duration),
	}

	for _, child := range span.Children {
		ee.Children = append(ee.Children, tracerSpanToExplainEntry(child))
	}

	return ee
}

func parseProxyError(e error) (*seqproxyapi.Error, bool) {
	if errors.Is(e, consts.ErrTooManyFractionsHit) {
		return &seqproxyapi.Error{
			Code:    seqproxyapi.ErrorCode_ERROR_CODE_TOO_MANY_FRACTIONS_HIT,
			Message: e.Error(),
		}, true
	}

	return nil, false
}

func shouldHaveResponse(code seqproxyapi.ErrorCode) bool {
	return code == seqproxyapi.ErrorCode_ERROR_CODE_NO || code == seqproxyapi.ErrorCode_ERROR_CODE_PARTIAL_RESPONSE
}
