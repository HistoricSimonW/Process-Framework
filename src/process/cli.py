from __future__ import annotations
import argparse, os, sys, json
from importlib.metadata import version as pkg_version
from .settings import Settings
from .pipeline import PipelineBuilder
from ._about import __app_name__, __app_version__

from process_model.pipeline.metadata import RunMetadata

def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="my-process", description="Run the My Process pipeline.")
    ap.add_argument("-c", "--config", default=os.getenv("PIPELINE_CONFIG", "config/default.yaml"),
                    help="Path to YAML config file")
    args = ap.parse_args(argv)

    settings = Settings(**args.__dict__)

    pipeline_builder = PipelineBuilder(print)

    meta = RunMetadata(
        process=__app_name__,
        process_version=__app_version__,
        process_model_version=safe_version("process_model")
    )

    pipeline = pipeline_builder.build_pipeline(settings, meta)

    pipeline.execute()

    return 0
    

def safe_version(dist: str) -> str:
    try:
        return pkg_version(dist)
    except Exception:
        return "0"