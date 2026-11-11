import logging
from abc import abstractmethod
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

from elasticsearch.helpers import BulkIndexError, streaming_bulk

from process_framework import Step, resolve
from process_framework.context_managers.logging import suppress_logging
from process_framework.steps.composition.core import HasInput, HasOptionalOutput
from process_framework.steps.composition.elasticsearch.core import (
    HasElasticsearch,
    HasElasticsearchIndex,
)


@dataclass(kw_only=True)
class BulkDocuments[TInput](
    HasElasticsearchIndex,
    HasElasticsearch,
    HasInput[Iterable[TInput]],
    HasOptionalOutput[Any],
    Step,
):
    """Base step for streaming Elasticsearch bulk operations."""

    pipeline: str | None = None
    assert_index_exists: bool = True

    raise_on_error: bool = True
    raise_on_exception: bool = True
    max_retries: int = 3
    initial_backoff: int = 2
    max_backoff: int = 60
    retry_on_status: tuple[int, ...] = (429, 502, 503, 504)

    chunk_size: int = 500
    max_chunk_bytes: int = 104_857_600
    bulk_kwargs: dict[str, Any] = field(default_factory=dict)

    log_progress_predicate: Callable[[int, int, list], bool] = (
        lambda seen, succeeded, errors: seen > 0 and seen % 1000 == 0
    )

    @property
    @abstractmethod
    def operation(self) -> str:
        """Return the bulk operation name."""
        ...

    @abstractmethod
    def get_actions(
        self,
        inputs: Iterable[TInput],
        index: str,
    ) -> Iterable[dict[str, Any]]:
        """Generate bulk actions from documents."""
        ...

    def get_bulk_kwargs(self) -> dict[str, Any]:
        """Combine bulk helper settings with additional options."""
        kwargs = {
            **self.bulk_kwargs,
            "raise_on_error": self.raise_on_error,
            "raise_on_exception": self.raise_on_exception,
            "max_retries": self.max_retries,
            "initial_backoff": self.initial_backoff,
            "max_backoff": self.max_backoff,
            "retry_on_status": self.retry_on_status,
            "chunk_size": self.chunk_size,
            "max_chunk_bytes": self.max_chunk_bytes,
            "yield_ok": True,
        }
        
        if "pipeline" in kwargs:
            raise ValueError(
                "Specify pipeline using the dedicated pipeline field."
            )
            
        if self.pipeline is not None:
            kwargs["pipeline"] = self.pipeline

        return kwargs
        
    def do(self) -> None:
        """Execute bulk actions and assign the operation result."""
        index = resolve(self.index)
        documents = resolve(self.input_)
        actions = self.get_actions(documents, index)

        succeeded = 0
        errors = []

        self._info(
            f"Bulk {self.operation} in {index} "
            f"with pipeline={self.pipeline!r}"
        )

        try:
            with suppress_logging(
                [
                    "elastic_transport.transport",
                    "urllib3.connectionpool",
                ],
                level=logging.WARNING,
            ):
                for seen, (ok, item) in enumerate(
                    streaming_bulk(
                        client=self.elasticsearch,
                        actions=actions,
                        **self.get_bulk_kwargs(),
                    ),
                    start=1,
                ):
                    if ok:
                        succeeded += 1
                    else:
                        errors.append(item)
                        self._error(f"{seen}: {item}")

                    if self.log_progress_predicate(
                        seen, succeeded, errors
                    ):
                        self._info(
                            f"{seen}: {self.operation}={succeeded}, "
                            f"errors={len(errors)}"
                        )

        except BulkIndexError as exc:
            for error in exc.errors:
                self._error(str(error))
            raise

        result = {
            self.operation: succeeded,
            "errors": errors,
        }

        self._info(
            f"Bulk {self.operation} completed: "
            f"{succeeded} succeeded, {len(errors)} errors"
        )

        if self.output_ is not None:
            self.output_.set_value(result)