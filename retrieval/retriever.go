package main

import (
	"context"
	"time"

	"go.uber.org/zap"
)

// Retriever handles RAG retrieval pipeline.
type Retriever struct {
	logger    *zap.SugaredLogger
	embedder  *EmbeddingService
	reranker  Reranker
}

// Retrieve fetches top-k relevant documents for a query.
func (r *Retriever) Retrieve(ctx context.Context, query string, topK int) ([]Document, error) {
	r.logger.Infof("Retrieve: query=%q topK=%d", query, topK)
	results, err := r.embedder.Query(ctx, query, topK*2)
	if err != nil {
		r.logger.Errorf("Retrieve: embedding query failed err=%v", err)
		return nil, err
	}
	reranked, err := r.reranker.Rerank(ctx, query, results)
	if err != nil {
		r.logger.Errorf("Retrieve: reranking failed results=%+v err=%v", results, err)
		return nil, err
	}
	r.logger.Infof("Retrieve: returning %d results", len(reranked))
	return reranked, nil
}

// WarmCache pre-fetches common queries to warm the embedding cache.
func (r *Retriever) WarmCache(ctx context.Context, queries []string) {
	for _, q := range queries {
		r.logger.Infof("WarmCache: warming query=%q", q)
		if _, err := r.embedder.Query(ctx, q, 5); err != nil {
			r.logger.Errorf("WarmCache: failed query=%q err=%v", q, err)
		}
	}
}

// MonitorLatency polls the embedding store latency every interval.
func (r *Retriever) MonitorLatency(ctx context.Context, interval time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
			start := time.Now()
			_, _ = r.embedder.Query(ctx, "health check", 1)
			latency := time.Since(start)
			r.logger.Infof("MonitorLatency: latency=%v", latency)
		}
	}
}

type Reranker interface {
	Rerank(ctx context.Context, query string, results []Result) ([]Document, error)
}

func (r *Retriever) ContinuousHealthCheck(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			result, err := r.embedder.Query(ctx, "ping", 1)
			if err != nil {
				r.logger.Errorf("ContinuousHealthCheck: ping failed err=%v", err)
			} else {
				r.logger.Infof("ContinuousHealthCheck: ok result=%+v", result)
			}
			r.logger.Infof("ContinuousHealthCheck: cycle complete result=%+v err=%v", result, err)
		}
	}
}
