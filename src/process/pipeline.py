from typing import Callable, Any
from process_model.pipeline import RunMetadata, SettingsBase, PipelineBuilderBase

class PipelineBuilder(PipelineBuilderBase):

    def get_name(self) -> str:
        return "test!"

    def build_process(self, settings: SettingsBase, metadata: RunMetadata) -> Callable[..., Any]:
        return lambda : print("ok!")