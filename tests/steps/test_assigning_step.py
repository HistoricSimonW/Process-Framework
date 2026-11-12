
from dataclasses import dataclass

from process_framework import AssigningStep, Reference


@dataclass(kw_only=True)
class GenerateNumber(AssigningStep[int]):
    value: int = 42

    def generate_value(self) -> int:
        return self.value


def test_assigning_step():
    output = Reference(int)
    step = GenerateNumber(output_=output)

    step.do()

    assert output.get_value() == 42


def test_assigning_step_overwrites():
    output = Reference(int, 10)
    step = GenerateNumber(
        output_=output, 
        value=42
    )

    step.do()

    assert output.get_value() == 42


def test_assigning_step_preserves_existing_value():
    output = Reference(int, 10)
    step = GenerateNumber(
        output_=output,
        value=42,
        overwrite=False,
    )

    step.do()

    assert output.get_value() == 10


def test_assigning_step_assigns_when_unset():
    output = Reference(int)
    step = GenerateNumber(
        output_=output,
        overwrite=False,
    )

    step.do()

    assert output.get_value() == 42
