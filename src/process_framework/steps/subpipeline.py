from process_framework import Step, SettingsBase, ReferencesBase, ClientsBase, ValueOrReference, resolve
from process_framework.pipeline.subpipeline import SubpipelineDefinitionBase
from abc import ABC, abstractmethod
from dataclasses import dataclass
from collections.abc import Iterable

@dataclass(kw_only=True)
class ExecuteSubPipeline[
        TSettings: SettingsBase,
        TInnerReferences: ReferencesBase,
        TContext,
        TClients: ClientsBase,
    ](Step, ABC):
    """Instantiate, validate, and execute a subpipeline."""

    definition:SubpipelineDefinitionBase[
        TSettings,
        TInnerReferences,
        TContext,
        TClients,
    ]
    
    def do(self):
        """Construct an instance of a subpipeline, preflight it and execute it"""
        context = self.initialize_context()
        pipeline = self.definition.instantiate_pipeline(context)
        pipeline.preflight()
        pipeline.do()
    
    @abstractmethod
    def initialize_context(self) -> TContext:
        """Initialize the execution context for this pipeline run"""
        ...
        

@dataclass(kw_only=True)
class ExecuteSubpipelineForEach[
        TIn,
        TSettings: SettingsBase,
        TInnerReferences: ReferencesBase,
        TClients: ClientsBase,
    ](Step, ABC):
    definition:SubpipelineDefinitionBase[
        TSettings,
        TInnerReferences,
        TIn,
        TClients,
    ]
    """Execute a subpipeline once for each input item."""
    
    items:ValueOrReference[Iterable[TIn]]
        
    def get_items(self) -> Iterable[TIn]:
        """Iterate the input items"""
        return resolve(self.items)

    def do(self):
        """For each item in the input, initialize a context and execute the pipeline"""
        for item in self.get_items():
            pipeline = self.definition.instantiate_pipeline(item)
            pipeline.preflight()
            pipeline.do()