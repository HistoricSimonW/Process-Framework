from dataclasses import dataclass
from typing import NamedTuple, Self

from process_framework import (
    IGettable,
    ISettable,
    ModifyingStep,
    Reference,
    Step,
)
from process_framework.pipeline import (
    ClientsBase,
    SettingsBase,
    SubprocessDefinitionBase,
    SubprocessReferencesBase,
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


class _UpperCase(ModifyingStep[str]):
    def transform_value(self, input_: str) -> str:
        return input_.upper()
    
class _UpperCaserContext(NamedTuple):
    input_:str
    output_:ISettable[str]
    
@dataclass(kw_only=True)
class _UpperCaserReferences(SubprocessReferencesBase[_UpperCaserContext]):
    in_:IGettable[str]
    out_:ISettable[str]
    
    @classmethod
    def initialize(cls, context: _UpperCaserContext) -> Self:
        return cls(
            in_=Reference(str, context.input_),
            out_=context.output_
        )
        
@dataclass
class _UpperCaserDefinition(SubprocessDefinitionBase[SettingsBase,_UpperCaserReferences, ClientsBase, _UpperCaserContext]):
    def get_references_type(self) -> type[_UpperCaserReferences]:
        return _UpperCaserReferences
    
    def instantiate_steps(self, references: _UpperCaserReferences) -> list[Step]:
        return [
            _UpperCase(
                input_=references.in_,
                output_=references.out_
            )
        ]
    