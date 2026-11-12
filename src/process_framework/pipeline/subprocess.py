from __future__ import annotations

# stdlib
from abc import abstractmethod, ABC
from dataclasses import dataclass

# first-party (process_framework / process)
from typing import Self, TYPE_CHECKING

from .clients import ClientsBase
from .pipeline import Pipeline
from .settings import SettingsBase
from .composition.core import ContainerBase


if TYPE_CHECKING:
    from ..steps import Step


@dataclass(kw_only=True)
class SubprocessReferencesBase[TContext](ContainerBase, ABC):
    """base class for reference containers holding subprocess state."""
    
    @classmethod
    @abstractmethod
    def initialize(cls, context:TContext) -> Self:
        """Construct a typed collection of References"""
        ...


@dataclass(kw_only=True)
class SubprocessDefinitionBase[TSettings:SettingsBase,
                               TReferences:SubprocessReferencesBase,
                               TClients:ClientsBase,
                               TContext](ABC):
    """ Defines a pipeline as a subprocess that can be instantiated from a caller-provided context.
        TReferences should derive from SubprocessReferencesBase[TContext] """
    settings:TSettings
    clients:TClients
    
    @abstractmethod
    def get_references_type(self) -> type[TReferences]:
        ...
        
        
    def initialize_references(self, context:TContext) -> TReferences:
        """Create references for a single subpipeline execution."""
        return self.get_references_type().initialize(context) # type: ignore[arg-type]
        

    @abstractmethod
    def instantiate_steps(self, references:TReferences) -> list[Step]:
        """Construct the ordered steps for this pipeline."""
        ...


    def instantiate_pipeline(self, context:TContext) -> Pipeline:
        """Instantiate a pipeline for the supplied execution context."""
        references = self.initialize_references(context)
        steps = self.instantiate_steps(references)
        return Pipeline(steps=steps)


if __name__ == '__main__':
    from typing import NamedTuple
    from process_framework import IGettable, ISettable, Reference
    from process_framework import ModifyingStep
    
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
        