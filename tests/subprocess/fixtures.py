
from dataclasses import dataclass
from typing import NamedTuple, Self

from process_framework import Reference, Step
from process_framework.pipeline import SettingsBase
from process_framework.pipeline import (
    SubprocessDefinitionBase,
    SubprocessReferencesBase,
    SettingsBase,
)
from process_framework.pipeline.clients import EmptyClients


type Events = list[tuple[str, str]]


class Context(NamedTuple):
    value: str
    events: Events


@dataclass(kw_only=True)
class SubprocessReferences(SubprocessReferencesBase[Context]):
    value: Reference[str]
    events: Events

    @classmethod
    def initialize(cls, context: Context) -> Self:
        return cls(
            value=Reference(str, context.value),
            events=context.events,
        )


@dataclass(kw_only=True)
class RecordStep(Step):
    value: Reference[str]
    events: Events

    def preflight(self) -> None:
        self.events.append(("preflight", self.value.get_value()))

    def do(self) -> None:
        self.events.append(("do", self.value.get_value()))


class SubprocessDefinition(
    SubprocessDefinitionBase[
        SettingsBase,
        SubprocessReferences,
        EmptyClients,
        Context,
    ]
):
    def get_references_type(self) -> type[SubprocessReferences]:
        return SubprocessReferences

    def instantiate_steps(
        self,
        references: SubprocessReferences,
    ) -> list[Step]:
        return [
            RecordStep(
                value=references.value,
                events=references.events,
            )
        ]


def make_definition() -> SubprocessDefinition:
    settings = SettingsBase()

    return SubprocessDefinition(
        settings=settings,
        clients=EmptyClients.initialize(settings),
    )
