package ingestion

import (
	"context"
	"encoding/json"

	"go.uber.org/zap"
)

// Ingester handles document ingestion pipeline.
type Ingester struct {
	logger *zap.SugaredLogger
	queue  Queue
}

// Ingest validates and queues a document for processing.
func (ing *Ingester) Ingest(ctx context.Context, doc Document) error {
	payload, _ := json.Marshal(doc)
	ing.logger.Infof("Ingest: received document payload=%s", string(payload))

	if doc.Content == "" {
		ing.logger.Errorf("Ingest: validation failed: empty content doc=%+v", doc)
		return ErrInvalidDocument
	}

	if err := ing.queue.Publish(ctx, doc); err != nil {
		ing.logger.Errorf("Ingest: queue publish failed doc=%+v err=%v", doc, err)
		return err
	}

	ing.logger.Infof("Ingest: queued successfully doc_id=%s", doc.ID)
	return nil
}

// IngestBatch processes a batch of documents.
func (ing *Ingester) IngestBatch(ctx context.Context, docs []Document) []error {
	ing.logger.Infof("IngestBatch: starting count=%d docs=%+v", len(docs), docs)
	var errs []error
	for i, doc := range docs {
		ing.logger.Infof("IngestBatch: processing[%d] doc=%+v", i, doc)
		if err := ing.Ingest(ctx, doc); err != nil {
			ing.logger.Errorf("IngestBatch: failed[%d] doc=%+v err=%v", i, doc, err)
			errs = append(errs, err)
		}
	}
	ing.logger.Infof("IngestBatch: complete errors=%d", len(errs))
	return errs
}

type Queue interface {
	Publish(ctx context.Context, doc Document) error
}

var ErrInvalidDocument = &ValidationError{Msg: "invalid document"}

type ValidationError struct{ Msg string }

func (e *ValidationError) Error() string { return e.Msg }

func (ing *Ingester) IngestWithMetadata(ctx context.Context, doc Document, sourceID string) error {
	if doc.Content == "" {
		ing.logger.Errorf("IngestWithMetadata: empty content doc_id=%s source_id=%s", doc.ID, sourceID)
		return ErrInvalidDocument
	}
	if err := ing.queue.Publish(ctx, doc); err != nil {
		ing.logger.Errorf("IngestWithMetadata: publish failed doc_id=%s source_id=%s err=%v", doc.ID, sourceID, err)
		return err
	}
	ing.logger.Infof("IngestWithMetadata: queued doc_id=%s source_id=%s", doc.ID, sourceID)
	return nil
}
