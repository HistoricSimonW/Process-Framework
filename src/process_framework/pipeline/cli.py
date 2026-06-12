# """"""""""""""""""""""""""""""""""""""""""""""""""""" #
#   Use this to run pipelines when Podman or Docker     #
#       aren't available                                #
#                                                       #
#   All we're doing is loading a dotenv                 #
#       at --env-file                                   #
#                                                       #
#   And configuring a local log file                    #
#       to store logs                                   #
#                                                       #
#   Then running the pipeline                           #
#       as per                                          #
#                                                       #
# """"""""""""""""""""""""""""""""""""""""""""""""""""" #

import logging
import sys
from logging.handlers import RotatingFileHandler
from logging import Logger
from process_framework.pipeline.pipeline import PipelineDefinitionBase, ClientsBase, ReferencesDefinitionBase
from pathlib import Path
from typing import Sequence
from abc import ABC


from process_framework.pipeline.settings import SettingsBase, CliArg
from dataclasses import dataclass
class CliSettings(SettingsBase):
    verbosity: int = CliArg("-v", action="count", default=0)
    log_console: bool = CliArg("--log-console", action="store_true", default=False)
    log_file: Path | None = CliArg("--log-file", default=None)
    log_max_bytes: int = CliArg("--log-max-bytes", default=10_485_760, type=int)
    log_backup_count: int = CliArg("--log-backup-count", default=10, type=int)

    def get_log_level(self) -> int:
        if self.verbosity <= 0:
            return logging.WARN
        
        if self.verbosity == 1:
            return logging.INFO
        
        return logging.DEBUG
        

@dataclass(kw_only=True)
class CliBase():
    definition:type[PipelineDefinitionBase]
    settings_type:type[SettingsBase]
    clients_type:type[ClientsBase]
    references_type:type[ReferencesDefinitionBase]

    cli_settings_type:type[CliSettings] = CliSettings

    def main(self, argsv: Sequence[str] | None = None) -> int:
        if argsv is None:
            argsv = sys.argv[1:]
        
        settings = self.cli_settings_type.from_environment(argsv)

        logger = self.configure_logging(settings, None)

        definition = self.definition.from_environment(
            t_settings=self.settings_type,
            t_references=self.references_type,
            t_clients=self.clients_type,
            argsv=argsv
        )

        pipeline = definition.instantiate_pipeline()

        pipeline.log_steps()
        pipeline.preflight()
        pipeline.do()

        return 0


    def configure_logging(self, settings:CliSettings, logger:Logger|None) -> Logger:
        # handle verbosity (-v => INFO, -vv => DEBUG)

        log_level = settings.get_log_level()

        logger = logger or logging.getLogger()
        logger.handlers.clear()
        logger.setLevel(log_level)
        
        format = logging.Formatter(
            fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        if settings.log_console:
            ch = logging.StreamHandler(sys.stdout)
            ch.setLevel(log_level)
            ch.setFormatter(format)
            logger.addHandler(ch)
        
        if (log_file := settings.log_file) and isinstance(log_file, Path):
            log_file.parent.mkdir(parents=True, exist_ok=True)
            fh = RotatingFileHandler(
                filename=str(log_file),
                maxBytes=settings.log_max_bytes,
                backupCount=settings.log_backup_count,
                encoding='utf-8',
            )
            fh.setLevel(log_level)
            fh.setFormatter(format)
            logger.addHandler(fh)

        return logger    