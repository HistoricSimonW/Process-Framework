from collections.abc import Iterable
from dataclasses import dataclass

from process_framework.steps.elasticsearch.composition.actions import DeleteAction

from .core import BulkDocuments
from typing import Any

@dataclass(kw_only=True)
class DeleteDocuments(BulkDocuments[str]):
    """Delete documents using Elasticsearch streaming bulk operations."""

    @property
    def operation(self) -> str:
        return "deleted"

    def get_actions(
        self,
        inputs: Iterable[str],
        index: str,
    ) -> Iterable[dict[str, Any]]:
        """Generate index actions from documents."""
        return DeleteAction.from_ids(inputs, index)
    
    
    def preflight(self) -> None:
        """Reject ingest pipelines for delete operations."""
        super().preflight()

        if self.pipeline is not None:
            raise ValueError(
                "Ingest pipelines are not supported for bulk delete operations."
            )