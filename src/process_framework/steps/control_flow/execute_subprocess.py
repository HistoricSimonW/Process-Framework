from collections.abc import Callable, Iterable
from dataclasses import dataclass

from process_framework import (
    ClientsBase,
    SettingsBase,
    Step,
    ValueOrReference,
    resolve,
)
from process_framework.pipeline import SubprocessDefinitionBase, SubprocessReferencesBase

from ...composition.retries import RetryArgs, wrap

@dataclass(kw_only=True)
class ExecuteSubprocess[TSettings: SettingsBase,
                        TReferences: SubprocessReferencesBase,
                        TClients: ClientsBase,
                        TContext](Step):
    """Execute a subprocess using context from the outer pipeline."""

    definition: SubprocessDefinitionBase[TSettings, 
                                         TReferences, 
                                         TClients,
                                         TContext] 
    context: ValueOrReference[TContext]
    retry:RetryArgs|None=None
    
    def get_context(self) -> TContext:
        """Resolve the subprocess context."""
        return resolve(self.context)


    def do(self) -> None:
        """Instantiate, validate, and execute the subprocess."""
        pipeline = self.definition.instantiate_pipeline(
            self.get_context()
        )
        pipeline.preflight()
        wrap(self.retry, pipeline.do)()


@dataclass(kw_only=True)
class ExecuteSubprocessForEach[TSettings: SettingsBase,
                               TReferences: SubprocessReferencesBase,
                               TClients: ClientsBase,
                               TItem](Step):
    """Execute a subprocess for each item from the outer pipeline."""

    definition: SubprocessDefinitionBase[TSettings, 
                                         TReferences, 
                                         TClients,
                                         TItem]
    items: ValueOrReference[Iterable[TItem]]
    retry:RetryArgs|None=None
    
    def get_items(self) -> Iterable[TItem]:
        """Resolve the items to process."""
        return resolve(self.items)

    def do(self) -> None:
        """Execute a subprocess for each item."""
        for item in self.get_items():
            pipeline = self.definition.instantiate_pipeline(item)
            pipeline.preflight()
            wrap(self.retry, pipeline.do)()


@dataclass(kw_only=True)
class ExecuteSubprocessForEachBatch[TSettings: SettingsBase,
                                    TReferences: SubprocessReferencesBase,
                                    TClients: ClientsBase,
                                    TInput,
                                    TItem](Step):
    """Generate items from an input and execute a subprocess for each."""

    definition: SubprocessDefinitionBase[TSettings, 
                                         TReferences, 
                                         TClients,
                                         TItem]
    input_: ValueOrReference[TInput]
    item_func: Callable[[TInput], Iterable[TItem]]
    retry:RetryArgs|None=None
    
    def gen_items(self) -> Iterable[TItem]:
        """Generate subprocess items from the resolved input."""
        return self.item_func(resolve(self.input_))

    def do(self) -> None:
        """Execute a subprocess for each generated item."""
        for item in self.gen_items():
            pipeline = self.definition.instantiate_pipeline(item)
            pipeline.preflight()
            wrap(self.retry, pipeline.do)()