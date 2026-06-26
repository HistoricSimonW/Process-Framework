from process_framework import Step, SettingsBase, ReferencesBase, ClientsBase
from process_framework.pipeline.subpipeline import SubpipelineDefinitionBase
from abc import ABC, abstractmethod
from dataclasses import dataclass

@dataclass
class ExecuteSubPipeline[
        TSettings: SettingsBase,
        TInnerReferences: ReferencesBase,
        TContext,
        TClients: ClientsBase,
    ](Step, ABC):
    definition:SubpipelineDefinitionBase[
        TSettings,
        TInnerReferences,
        TContext,
        TClients,
    ]
    
    def do(self):
        """ construct an instance of a subpipeline, preflight it and execute it """
        context = self.initialize_context()
        pipeline = self.definition.instantiate_pipeline(context)
        pipeline.preflight()
        pipeline.do()
    
    @abstractmethod
    def initialize_context(self) -> TContext:
        """ initialize the execution context for this pipeline run """
        ...
        
from typing import Any
from process_framework import IGettable
from collections.abc import Iterable
@dataclass
class ExecuteSubpipelineForEach[
        TIn,
        TSettings: SettingsBase,
        TInnerReferences: ReferencesBase,
        TContext,
        TClients: ClientsBase,
    ](Step, ABC):
    definition:SubpipelineDefinitionBase[
        TSettings,
        TInnerReferences,
        TContext,
        TClients,
    ]
    items:IGettable[Iterable[TIn]]|Iterable[TIn]
    
    @abstractmethod
    def initialize_context(self, item:TIn) -> TContext:
        """ initialize the execution context for this pipeline run with the provided input"""
        ...
        
    def get_items(self) -> Iterable[TIn]:
        return self.items.get_value() if isinstance(self.items, IGettable) else self.items
        
    def do(self):
        for item in self.get_items():
            context = self.initialize_context(item)
            pipeline = self.definition.instantiate_pipeline(context)
            pipeline.preflight()
            pipeline.do()