from abc import ABC
from ..composition.mixins.logging import HasLogger

class PreflightTerminal:
    """terminal no-op for preflight chains."""
    def preflight(self):
        pass


class Preflightable(ABC, PreflightTerminal, HasLogger):
    """base class enabling cooperative preflight chaining and logging."""
    def preflight(self) -> None:
        super().preflight()