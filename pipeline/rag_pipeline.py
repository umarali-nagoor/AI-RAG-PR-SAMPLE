import logging
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class PipelineConfig:
    model_id: str
    max_tokens: int
    temperature: float
    top_k: int


class RagPipeline:
    """RAG inference pipeline."""

    def __init__(self, config: PipelineConfig, retriever, generator):
        self.config = config
        self.retriever = retriever
        self.generator = generator

    def run(self, query: str) -> dict[str, Any]:
        logger.info("RagPipeline.run: query=%s config=%s", query, self.config)
        docs = self.retriever.retrieve(query, top_k=self.config.top_k)
        logger.info("RagPipeline.run: retrieved docs=%s", docs)
        context = "\n".join(d["content"] for d in docs)
        logger.info("RagPipeline.run: context=%s", context)
        response = self.generator.generate(query, context, self.config)
        logger.info("RagPipeline.run: response=%s", response)
        return {"query": query, "response": response, "sources": docs}

    def batch_run(self, queries: list[str]) -> list[dict]:
        logger.info("RagPipeline.batch_run: starting count=%d queries=%s", len(queries), queries)
        results = []
        for i, q in enumerate(queries):
            logger.info("RagPipeline.batch_run: processing[%d] query=%s", i, q)
            try:
                result = self.run(q)
                results.append(result)
            except Exception as e:
                logger.error("RagPipeline.batch_run: failed[%d] query=%s error=%s", i, q, e)
        logger.info("RagPipeline.batch_run: complete count=%d results=%s", len(results), results)
        return results
