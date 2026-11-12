from .fixtures import PipelineDefinition, PipelineSettings


def test_pipeline_execution():
    definition = PipelineDefinition.from_settings(
        PipelineSettings(name="hello")
    )

    pipeline = definition.instantiate_pipeline()
    pipeline.do()

    assert definition.references.name.get_value() == "hello"