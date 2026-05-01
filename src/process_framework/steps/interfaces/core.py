from abc import abstractmethod, ABC

class IGenerateValue[T](ABC):
    """interface for producing a value."""
    @abstractmethod
    def generate_value(self) -> T|None:
        ...


class ITransformValue[TIn, TOut](ABC):
    """interface for transforming an input value into an output value."""
    @abstractmethod
    def transform_value(self, input_:TIn) -> TOut:
        ...