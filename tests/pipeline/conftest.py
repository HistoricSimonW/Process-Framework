import pytest

from .fixtures import PipelineDefinition, PipelineSettings


@pytest.fixture
def settings() -> PipelineSettings:
    return PipelineSettings(name="test")


@pytest.fixture
def definition(settings: PipelineSettings) -> PipelineDefinition:
    return PipelineDefinition.from_settings(settings)