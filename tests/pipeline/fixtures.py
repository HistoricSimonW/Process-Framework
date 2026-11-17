from collections.abc import Generator

from process_framework import PipelineDefinitionBase, SettingsBase, PipelineReferencesBase, Reference, AssigningStep, ValueOrReference, resolve
from process_framework._helpers import do
from process_framework.steps import Step
from process_framework.steps.logging_step import LogInput
from process_framework.pipeline.clients import EmptyClients
from process_framework.pipeline.script import ScriptBase
from dataclasses import dataclass
from typing import Self, cast


class PipelineSettings(SettingsBase):
    name:str


@dataclass
class PipelineReferences(PipelineReferencesBase):
    name:Reference[str]
    
    @classmethod
    def initialize(cls) -> Self:
        return cls(
            name=Reference(str)
        )
        
        
@dataclass
class AssignName(AssigningStep[str]):
    name:ValueOrReference[str]

    def generate_value(self) -> str | None:
        return resolve(self.name)
    
    
class PipelineDefinition(PipelineDefinitionBase[PipelineSettings, PipelineReferences, EmptyClients]):
    @classmethod
    def get_settings_type(cls) -> type[PipelineSettings]:
        return PipelineSettings
    
    @classmethod
    def get_references_type(cls) -> type[PipelineReferences]:
        return PipelineReferences
    
    @classmethod
    def get_clients_type(cls) -> type[EmptyClients]:
        return EmptyClients
    
    def generate_steps(self) -> Generator[Step, None, None]:
        yield AssignName(
            name=self.settings.name,
            output_=self.references.name
        )
        yield LogInput(
            input_=self.references.name
        )

@dataclass
class ScriptDefinition(ScriptBase[PipelineSettings, EmptyClients]):
    """Test script using an existing step with a local reference."""

    result: str | None = None

    @classmethod
    def get_settings_type(cls) -> type[PipelineSettings]:
        return PipelineSettings

    @classmethod
    def get_clients_type(cls) -> type[EmptyClients]:
        return EmptyClients

    def do(self) -> None:
        output = Reference(str)

        do(
            AssignName(
                name=self.settings.name,
                output_=output,
            ), preflight=True
        )

        self.result = output.get_value()
