package storeapi

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"time"

	"go.opencensus.io/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ozontech/seq-db/config"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/querytracer"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tracing"
	"github.com/ozontech/seq-db/util"
)

func (g *GrpcV1) Search(ctx context.Context, req *storeapi.SearchRequest) (*storeapi.SearchResponse, error) {
	ctx, span := tracing.StartSpan(ctx, "store-server/Search")
	defer span.End()

	if span.IsRecordingEvents() {
		span.AddAttributes(trace.StringAttribute("request", req.Query))
		span.AddAttributes(trace.Int64Attribute("from", req.From))
		span.AddAttributes(trace.Int64Attribute("to", req.From))
		span.AddAttributes(trace.Int64Attribute("size", req.Size))
		span.AddAttributes(trace.Int64Attribute("offset", req.Offset))
		span.AddAttributes(trace.Int64Attribute("interval", req.Interval))
		span.AddAttributes(trace.BoolAttribute("explain", req.Explain))
		span.AddAttributes(trace.BoolAttribute("with_total", req.WithTotal))
		span.AddAttributes(trace.StringAttribute("aggregation_filter", req.AggregationFilter))
	}

	tr := querytracer.New(req.Explain, "store/Search")
	data, err := g.doSearch(ctx, req, tr)
	if err != nil {
		span.SetStatus(trace.Status{Code: 1, Message: err.Error()})
		logger.Error("search error", zap.Error(err), zap.Object("request", (*searchRequestMarshaler)(req)))
	}

	tr.Done()
	if req.Explain && data != nil {
		data.Explain = tracerSpanToExplainEntry(tr.ToSpan())
	}

	return data, err
}

var aggAsteriskFilter = "*"

func (g *GrpcV1) doSearch(
	ctx context.Context,
	req *storeapi.SearchRequest,
	tr *querytracer.Tracer,
) (*storeapi.SearchResponse, error) {
	metric.SearchInFlightQueriesTotal.Inc()
	defer metric.SearchInFlightQueriesTotal.Dec()

	inflightRequests := g.searchData.inflight.Inc()
	defer g.searchData.inflight.Dec()

	if inflightRequests > int64(g.config.Search.RequestsLimit) {
		metric.RejectedRequests.WithLabelValues("search", "limit_exceeding").Inc()
		return nil, fmt.Errorf("too many search requests: %d > %d", inflightRequests, g.config.Search.RequestsLimit)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	start := time.Now()

	from := seq.MID(req.From)

	// in store mode hot we return error in case request wants data, that we've already rotated
	if g.config.StoreMode == StoreModeHot {
		if g.fracManager.Mature() && g.earlierThanOldestFrac(uint64(from)) {
			metric.RejectedRequests.WithLabelValues("search", "old_data").Inc()
			return &storeapi.SearchResponse{Code: storeapi.SearchErrorCode_INGESTOR_QUERY_WANTS_OLD_DATA}, nil
		}
	}

	to := seq.MID(req.To)
	limit := int(req.Size + req.Offset)

	if req.Explain {
		logger.Info("search request will be explained", zap.Any("request", req))
	}

	t := time.Now()

	parseQueryTr := tr.NewChild("parse query")
	ast, err := g.parseQuery(ctx, req.Query)
	parseQueryTr.Done()
	if err != nil {
		if code, ok := parseStoreError(err); ok {
			return &storeapi.SearchResponse{Code: code}, nil
		}
		return nil, err
	}

	if util.IsCancelled(ctx) {
		return nil, fmt.Errorf("search cancelled before evaluating: reason=%w", ctx.Err())
	}

	aggQ, err := aggQueriesFromProto(req.Aggs)
	if err != nil {
		return nil, err
	}

	const millisecondsInSecond = float64(time.Second / time.Millisecond)
	metric.SearchRangesSeconds.Observe(float64(to-from) / millisecondsInSecond)

	searchParams := processor.SearchParams{
		AST:          ast,
		AggQ:         aggQ,
		HistInterval: uint64(req.Interval),
		From:         from,
		To:           to,
		Limit:        limit,
		WithTotal:    req.WithTotal,
		Order:        req.Order.MustDocsOrder(),
	}

	searchTr := tr.NewChild("search iteratively")
	qpr, err := g.searchData.searcher.SearchDocs(
		ctx,
		g.fracManager.GetAllFracs(),
		searchParams,
	)
	searchTr.Done()
	if err != nil {
		if code, ok := parseStoreError(err); ok {
			return &storeapi.SearchResponse{Code: code}, nil
		}

		return nil, err
	}

	metric.SearchDurationSeconds.Observe(time.Since(start).Seconds())

	if req.Explain {
		if req.Interval > 0 {
			keys := make([]uint64, 0, len(qpr.Histogram))
			for key := range qpr.Histogram {
				keys = append(keys, uint64(key))
			}
			slices.Sort(keys)

			for _, key := range keys {
				logger.Info("histogram",
					zap.Int64("t", t.UnixNano()),
					zap.String("q", req.Query),
					zap.Uint64("key", key),
					zap.Uint64("val", qpr.Histogram[seq.MID(key)]),
				)
			}
		}
	}

	took := time.Since(start)
	if g.config.Search.LogThreshold != 0 && took >= g.config.Search.LogThreshold {
		logger.Warn("slow search",
			zap.Int64("took_ms", took.Milliseconds()),
			zap.Object("req", (*searchRequestMarshaler)(req)),
			zap.Uint64("found", qpr.Total),
			zap.String("from", seq.MID(req.From).String()),
			zap.String("to", seq.MID(req.To).String()),
			zap.Int64("offset", req.Offset),
			zap.Int64("size", req.Size),
			zap.Bool("with_total", req.WithTotal),
		)
	}

	return buildSearchResponse(qpr), nil
}

func (g *GrpcV1) parseQuery(ctx context.Context, query string) (*parser.ASTNode, error) {
	if query == "" {
		query = seq.TokenAll + ":*"
	}
	var ast *parser.ASTNode
	if useSeqQL(ctx) {
		seqql, err := parser.ParseSeqQL(query, g.mappingProvider.GetMapping())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "can't parse query %q: %v", query, err)
		}
		ast = seqql.Root
	} else {
		var err error
		ast, err = parser.ParseQuery(query, g.mappingProvider.GetMapping())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "can't parse query %q: %v", query, err)
		}
	}
	return ast, nil
}

func useSeqQL(ctx context.Context) bool {
	md, _ := metadata.FromIncomingContext(ctx)
	useSeqQLValues := md.Get("use-seq-ql")
	if len(useSeqQLValues) == 0 {
		// Header isn't set, so use default query language.
		return config.UseSeqQLByDefault
	}
	val := useSeqQLValues[0]
	useSeqQL, _ := strconv.ParseBool(val)
	return useSeqQL
}

func (g *GrpcV1) earlierThanOldestFrac(from uint64) bool {
	oldestCt := g.fracManager.OldestCT.Load()
	return oldestCt == 0 || oldestCt > from
}

func buildSearchResponse(qpr *seq.QPR) *storeapi.SearchResponse {
	idSourcesBuf := make([]storeapi.SearchResponse_IdWithHint, len(qpr.IDs))
	idSources := make([]*storeapi.SearchResponse_IdWithHint, len(qpr.IDs))
	for i := range qpr.IDs {
		idSourcesBuf[i].Id = &storeapi.SearchResponse_Id{
			Mid: uint64(qpr.IDs[i].ID.MID),
			Rid: uint64(qpr.IDs[i].ID.RID),
		}
		idSourcesBuf[i].Hint = qpr.IDs[i].Hint

		idSources[i] = &idSourcesBuf[i]
	}

	// convert map[MID]uint64 -> map[uint64]uint64
	// because Go can not convert MID to uint64
	// and protobuf can not use our MID data type
	histogram := make(map[uint64]uint64, len(qpr.Histogram))
	for k, v := range qpr.Histogram {
		histogram[uint64(k)] = v
	}

	aggsBuf := make([]storeapi.SearchResponse_Agg, len(qpr.Aggs))
	aggs := make([]*storeapi.SearchResponse_Agg, len(qpr.Aggs))

	for i, fromAgg := range qpr.Aggs {
		curAgg := &aggsBuf[i]

		from := fromAgg.SamplesByBin
		to := make(map[string]*storeapi.SearchResponse_Histogram, len(from))

		// TODO(dkharms): We can remove this variable since [Agg] is deprecated.
		toAgg := make(map[string]uint64, len(from))

		for bin, hist := range from {
			pbhist := &storeapi.SearchResponse_Histogram{
				Min:       hist.Min,
				Max:       hist.Max,
				Sum:       hist.Sum,
				Total:     hist.Total,
				Samples:   hist.Samples,
				NotExists: hist.NotExists,
			}

			curAgg.Timeseries = append(curAgg.Timeseries,
				&storeapi.SearchResponse_Bin{
					Label: bin.Token,
					Ts:    timestamppb.New(bin.MID.Time()),
					Hist:  pbhist,
				},
			)

			to[bin.Token] = pbhist
			toAgg[bin.Token] = uint64(hist.Total)
		}

		curAgg.NotExists = fromAgg.NotExists
		curAgg.AggHistogram = to
		curAgg.Agg = toAgg

		aggs[i] = curAgg
	}

	return &storeapi.SearchResponse{
		IdSources: idSources,
		Histogram: histogram,
		Aggs:      aggs,
		Total:     qpr.Total,
	}
}

func aggQueriesFromProto(aggs []*storeapi.AggQuery) ([]processor.AggQuery, error) {
	aggQ := make([]processor.AggQuery, 0, len(aggs))
	for _, aggQuery := range aggs {
		aggFunc, err := aggQueryFromProto(aggQuery)
		if err != nil {
			return nil, err
		}
		aggQ = append(aggQ, aggFunc)
	}
	return aggQ, nil
}

func aggQueryFromProto(aggQuery *storeapi.AggQuery) (processor.AggQuery, error) {
	// 'groupBy' is required for Count and Unique.
	if aggQuery.GroupBy == "" && (aggQuery.Func == storeapi.AggFunc_AGG_FUNC_COUNT || aggQuery.Func == storeapi.AggFunc_AGG_FUNC_UNIQUE) {
		return processor.AggQuery{}, fmt.Errorf("%w: groupBy is required for %s func", consts.ErrInvalidAggQuery, aggQuery.Func)
	}

	// 'field' is required for stat functions like sum, avg, max and min.
	if aggQuery.Field == "" && aggQuery.Func != storeapi.AggFunc_AGG_FUNC_COUNT && aggQuery.Func != storeapi.AggFunc_AGG_FUNC_UNIQUE {
		return processor.AggQuery{}, fmt.Errorf("%w: field is required for %s func", consts.ErrInvalidAggQuery, aggQuery.Func)
	}

	// Check 'quantiles' is not empty for Quantile func.
	if len(aggQuery.Quantiles) == 0 && aggQuery.Func == storeapi.AggFunc_AGG_FUNC_QUANTILE {
		return processor.AggQuery{}, fmt.Errorf("%w: expect an argument for Quantile func", consts.ErrInvalidAggQuery)
	}

	var field *parser.Literal
	if aggQuery.Field != "" {
		field = &parser.Literal{
			Field: aggQuery.Field,
			Terms: searchAll,
		}
	}

	var groupBy *parser.Literal
	if aggQuery.GroupBy != "" {
		groupBy = &parser.Literal{
			Field: aggQuery.GroupBy,
			Terms: searchAll,
		}
	}

	aggFunc, err := aggQuery.Func.ToAggFunc()
	if err != nil {
		return processor.AggQuery{}, err
	}

	return processor.AggQuery{
		Field:     field,
		GroupBy:   groupBy,
		Func:      aggFunc,
		Interval:  aggQuery.Interval,
		Quantiles: aggQuery.Quantiles,
	}, nil
}

var searchAll = []parser.Term{{
	Kind: parser.TermSymbol, Data: aggAsteriskFilter,
}}

type aggQueryMarshaler storeapi.AggQuery

func (s *aggQueryMarshaler) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("field", s.Field)
	enc.AddString("groupBy", s.GroupBy)
	enc.AddString("func", s.Func.String())
	enc.AddInt("quantiles_count", len(s.Quantiles))
	return nil
}

type aggQuerySliceMarshaler []*storeapi.AggQuery

func (s aggQuerySliceMarshaler) MarshalLogArray(enc zapcore.ArrayEncoder) error {
	for _, q := range s {
		_ = enc.AppendObject((*aggQueryMarshaler)(q))
	}
	return nil
}

type searchRequestMarshaler storeapi.SearchRequest

func (s *searchRequestMarshaler) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("query", s.Query)
	enc.AddString("aggregation_filter", s.AggregationFilter)
	enc.AddString("aggregation", s.Aggregation)
	_ = enc.AddArray("aggs", aggQuerySliceMarshaler(s.Aggs))

	enc.AddString("from", util.MsTsToESFormat(uint64(s.From)))
	enc.AddString("to", util.MsTsToESFormat(uint64(s.To)))

	enc.AddInt64("size", s.Size)
	enc.AddInt64("offset", s.Offset)
	enc.AddInt64("interval", s.Interval)
	enc.AddBool("explain", s.Explain)
	enc.AddBool("with_total", s.WithTotal)

	return nil
}
