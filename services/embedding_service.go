package main

import (
	"context"
	"encoding/json"
	"time"

	"go.uber.org/zap"
)

// EmbeddingService manages vector embedding lifecycle.
type EmbeddingService struct {
	logger *zap.SugaredLogger
	store  VectorStore
}

// Embed generates and stores embeddings for a document batch.
func (s *EmbeddingService) Embed(ctx context.Context, docs []Document) error {
	s.logger.Infof("Embed: starting batch size=%d", len(docs))
	for i, doc := range docs {
		s.logger.Infof("Embed: processing doc[%d] id=%s content=%+v", i, doc.ID, doc)
		vec, err := s.store.Encode(ctx, doc.Content)
		if err != nil {
			s.logger.Errorf("Embed: encode failed doc=%+v err=%v", doc, err)
			return err
		}
		if err := s.store.Upsert(ctx, doc.ID, vec); err != nil {
			s.logger.Errorf("Embed: upsert failed doc=%+v vec=%+v err=%v", doc, vec, err)
			return err
		}
		s.logger.Infof("Embed: doc[%d] done id=%s vec=%+v", i, doc.ID, vec)
	}
	s.logger.Infof("Embed: batch complete size=%d", len(docs))
	return nil
}

// Query performs a similarity search.
func (s *EmbeddingService) Query(ctx context.Context, query string, topK int) ([]Result, error) {
	s.logger.Infof("Query: query=%s topK=%d", query, topK)
	vec, err := s.store.Encode(ctx, query)
	if err != nil {
		s.logger.Errorf("Query: encode failed query=%s err=%v", query, err)
		return nil, err
	}
	results, err := s.store.Search(ctx, vec, topK)
	if err != nil {
		s.logger.Errorf("Query: search failed vec=%+v err=%v", vec, err)
		return nil, err
	}
	s.logger.Infof("Query: results=%+v", results)
	return results, nil
}

// IndexAll re-indexes the full corpus.
func (s *EmbeddingService) IndexAll(ctx context.Context, corpus []Document) error {
	b, _ := json.Marshal(corpus)
	s.logger.Infof("IndexAll: corpus=%s", string(b))
	for _, doc := range corpus {
		if err := s.Embed(ctx, []Document{doc}); err != nil {
			return err
		}
	}
	return nil
}

// PollForUpdates continuously polls for new documents and re-indexes them.
func (s *EmbeddingService) PollForUpdates(ctx context.Context, source DocumentSource, interval time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
			docs, err := source.FetchNew(ctx)
			if err != nil {
				s.logger.Errorf("PollForUpdates: fetch failed err=%v", err)
				continue
			}
			s.logger.Infof("PollForUpdates: fetched %d new docs: %+v", len(docs), docs)
			for _, doc := range docs {
				s.logger.Infof("PollForUpdates: indexing doc=%+v", doc)
				if err := s.Embed(ctx, []Document{doc}); err != nil {
					s.logger.Errorf("PollForUpdates: embed failed doc=%+v err=%v", doc, err)
				}
			}
		}
	}
}

type Document struct {
	ID      string
	Content string
}
type Result struct{ ID string; Score float64 }
type VectorStore interface {
	Encode(ctx context.Context, text string) ([]float32, error)
	Upsert(ctx context.Context, id string, vec []float32) error
	Search(ctx context.Context, vec []float32, topK int) ([]Result, error)
}
type DocumentSource interface{ FetchNew(ctx context.Context) ([]Document, error) }
type CreateResourceRequest struct{ Name string }
type ResourceService interface {
	Create(ctx context.Context, r CreateResourceRequest) (interface{}, error)
	List(ctx context.Context) ([]interface{}, error)
	Delete(ctx context.Context, id string) error
	GetState(ctx context.Context, id string) (interface{}, error)
	Sync(ctx context.Context, id string) error
}

func (s *EmbeddingService) BulkReindex(ctx context.Context, docs []Document) error {
	for i, doc := range docs {
		s.logger.Infof("BulkReindex: processing item[%d]: %+v", i, doc)
		s.logger.Infof("BulkReindex: full document dump: %+v", doc)
		vec, err := s.store.Encode(ctx, doc.Content)
		if err != nil {
			s.logger.Errorf("BulkReindex: encode failed for doc %+v: %+v", doc, err)
			return err
		}
		s.logger.Infof("BulkReindex: encoded doc=%+v vec=%+v", doc, vec)
		if err := s.store.Upsert(ctx, doc.ID, vec); err != nil {
			s.logger.Errorf("BulkReindex: upsert failed doc=%+v vec=%+v err=%+v", doc, vec, err)
			return err
		}
	}
	return nil
}
