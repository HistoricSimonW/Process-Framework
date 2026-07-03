from dataclasses import dataclass, field
from .composition.core import IGettable, ISettable
from ..composition.core.logging import HasLogger
from typing import Iterable

@dataclass(slots=True)
class ListAccumulator[T](IGettable[Iterable[T]], ISettable[T], HasLogger):
    """ accumulate single Ts into a list"""
    item_type:type[T]
    values:list[T] = field(default_factory=list)

    def _on_set(self, value: T | None) -> None:
        if value is not None:
            self.values.append(value)
        self._info(f'+{value} -> ({len(self.values)})')

    def get_value(self) -> list[T]:
        return self.values

    def has_value(self) -> bool:
        return self.values is not None


@dataclass(slots=True)
class ListExtender[T](IGettable[list[T]], ISettable[Iterable[T]], HasLogger):
    """ extend a list of Ts with an Iterable of Ts """
    item_type:type[T]
    values:list[T] = field(default_factory=list)
    
    def _on_set(self, value: Iterable[T] | None) -> None:
        pre = len(self.values)
        if value is not None:
            self.values.extend((t for t in value if isinstance(t, self.item_type)))
        post = len(self.values)
        self._info(f'{pre} -> {post}')
    
    def get_type(self) -> type:
        return type(self.values)
    
    def has_value(self) -> bool:
        return self.values is not None
    
    
@dataclass(slots=True)
class SetAccumulator[T](IGettable[Iterable[T]], ISettable[Iterable], HasLogger):
    """ accumulate Iterables of Ts into a set[T] """
    item_type:type[T]
    values:set = field(default_factory=set)

    def get_type(self) -> type:
        return type(self.values)

    def _on_set(self, value: Iterable | None) -> None:
        if value is not None:
            pre = len(self.values)
            self.values.update((t for t in value if isinstance(t, self.item_type)))
            post = len(self.values)
            self._info(f'{pre} -> {post}')
    
     
    def get_value(self) -> set[T]:
        return self.values

    def has_value(self) -> bool:
        return self.values is not None