from collections.abc import Iterable
from dataclasses import dataclass

from process_framework.steps.composition.elasticsearch.actions import IndexAction
from process_framework.steps.composition.elasticsearch.document import DocumentBase

from .core import BulkDocuments
from typing import Any

@dataclass(kw_only=True)
class IndexDocuments[T:DocumentBase](BulkDocuments[T]):
    """Index documents using Elasticsearch streaming bulk operations."""

    @property
    def operation(self) -> str:
        return "indexed"

    def get_actions(
        self,
        inputs: Iterable[T],
        index: str,
    ) -> Iterable[dict[str, Any]]:
        """Generate index actions from documents."""
        return IndexAction.from_documents(inputs, index)