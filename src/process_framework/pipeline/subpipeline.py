
# stdlib
from abc import abstractmethod, ABC
from typing import Self
from dataclasses import dataclass

# first-party (process_framework / process)
from ..steps import Step
from .clients import ClientsBase
from .references import ReferencesBase
from .settings import SettingsBase
from .pipeline import Pipeline
from typing import NamedTuple

@dataclass(kw_only=True)
class SubpipelineDefinitionBase[
        TSettings:SettingsBase,
        TReferences:ReferencesBase,
        TContext,
        TClients:ClientsBase
    ](ABC):
    """Defines a pipeline that can be instantiated from a caller-provided context."""
    settings:TSettings
    clients:TClients
    

    @abstractmethod
    def initialize_references(self, context:TContext) -> TReferences:
        """Create references for a single subpipeline execution."""
        ...
        

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
    from process_framework import IGettable, ISettable, Reference
    @dataclass(kw_only=True)
    class _UpperCaserReferences(ReferencesBase):
        in_:IGettable[str]
        out_:ISettable[str]

    from process_framework import ModifyingStep
    class _UpperCase(ModifyingStep[str]):
        def transform_value(self, input_: str) -> str:
            return input_.upper()
        
    class _UpperCaserContext(NamedTuple):
        input_:str
        output_:ISettable[str]
        
    @dataclass
    class _UpperCaserDefinition(SubpipelineDefinitionBase[SettingsBase,_UpperCaserReferences, _UpperCaserContext , ClientsBase]):
        def initialize_references(self, context: _UpperCaserContext) -> _UpperCaserReferences:
            return _UpperCaserReferences(
                in_=Reference(str, context.input_),
                out_=context.output_
            )
        
        def instantiate_steps(self, references: _UpperCaserReferences) -> list[Step]:
            return [
                _UpperCase(
                    input_=references.in_,
                    output_=references.out_
                )
            ]
        