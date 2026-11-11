from collections.abc import Iterable
from dataclasses import dataclass

from process_framework.steps.composition.elasticsearch.actions import UpdateAction
from process_framework.steps.composition.elasticsearch.document import DocumentBase

from .core import BulkDocuments
from typing import Any

@dataclass(kw_only=True)
class UpdateDocuments[T:DocumentBase](BulkDocuments[T]):
    """Partially update documents using Elasticsearch streaming bulk operations."""

    doc_as_upsert: bool = False

    @property
    def operation(self) -> str:
        return "updated"

    def get_actions(
        self,
        inputs: Iterable[T],
        index: str,
    ) -> Iterable[dict[str, Any]]:
        """Generate partial update actions from documents."""
        return UpdateAction.from_documents(inputs, index, doc_as_upsert=self.doc_as_upsert)
    
    
    def preflight(self) -> None:
        super().preflight()
        
        pipeline = self.pipeline or self.bulk_kwargs.get('pipeline')
        
        if self.doc_as_upsert and pipeline is not None:
            raise ValueError(
                "doc_as_upsert=True is not supported with ingest pipelines."
            )