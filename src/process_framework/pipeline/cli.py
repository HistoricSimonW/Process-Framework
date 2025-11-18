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

from argparse import ArgumentParser, Namespace
import logging
import sys
from logging.handlers import RotatingFileHandler
from logging import Logger
from process_framework.pipeline import PipelineBase
from pathlib import Path
from typing import Sequence
from abc import ABC, abstractmethod

class CliBase[TPipeline:PipelineBase](ABC):
    
    def main(self, argsv:Sequence[str]|None=None) -> int:
        # if argsv haven't been passed in, get them from sys
        if argsv is None:
            argsv = sys.argv[1:]

        # parse args for cli
        parser = ArgumentParser()
        self.add_args(parser)
        args, _ = parser.parse_known_args(argsv)

        # configure logging
        logger = logging.getLogger()
        self.configure_logging(args, logger)

        # init pipeline
        pipeline = self.initialize_pipeline(argsv)
        pipeline.log_steps()
        pipeline.do()
        return 0
    
        
    def initialize_pipeline(self, argsv:Sequence[str]|None) -> TPipeline:
        cls = self.get_pipeline_class()
        return cls(argsv)
    

    @abstractmethod
    def get_pipeline_class(self) -> type[TPipeline]:
        """ get the type of the Pipeline to run """
        ...


    def configure_logging(self, args:Namespace, logger:Logger|None) -> Logger:
        # handle verbosity (-v => INFO, -vv => DEBUG)
        verbosity = args.verbosity
        level = logging.WARNING
        if verbosity >= 2:
            level = logging.DEBUG
        elif verbosity >= 1:
            level = logging.INFO

        logger = logger or logging.getLogger()
        logger.handlers.clear()
        logger.setLevel(level)
        
        format = logging.Formatter(
            fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        if args.log_console:
            ch = logging.StreamHandler(sys.stdout)
            ch.setLevel(level)
            ch.setFormatter(format)
            logger.addHandler(ch)
        
        if (log_file := args.log_file) and isinstance(log_file, Path):
            log_file.parent.mkdir(parents=True, exist_ok=True)
            fh = RotatingFileHandler(
                filename=str(log_file),
                maxBytes=args.log_max_bytes,
                backupCount=args.log_backup_count,
                encoding='utf-8',
            )
            fh.setLevel(level)
            fh.setFormatter(format)
            logger.addHandler(fh)

        return logger


    def add_args(self, parser:ArgumentParser):
        # this can be overwritten to add more or different arguments
        # but usually we only need it to set up logging in the cli context
        # logging
        parser.add_argument('--verbosity', '-v', action='count', default=1, help='-v for INFO, -vv for DEBUG')
        parser.add_argument('--log-console', action='store_true', default=False)
        parser.add_argument('--log-file', type=Path, default=None,
                    help='Path to a rotating log file (disabled if omitted)')
        parser.add_argument('--log-max-bytes', type=int, default=10_485_760,  # 10 MB
                        help='Max bytes before size rotation')
        parser.add_argument('--log-backup-count', type=int, default=10,
                        help='How many rotated files to keep')
    