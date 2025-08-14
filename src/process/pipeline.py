from typing import Callable, Any
from process_model.pipeline import RunMetadata, SettingsBase, PipelineBuilderBase

class PipelineBuilder(PipelineBuilderBase):

    def get_name(self) -> str:
        raise NotImplementedError()

    def build_process(self, settings: SettingsBase, metadata: RunMetadata) -> Callable[..., Any]:
        raise NotImplementedError()