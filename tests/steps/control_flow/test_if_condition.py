
from dataclasses import dataclass, field
from typing import Any

import pytest

from process_framework import Reference, Step
from process_framework.steps.control_flow.if_condition import (
    Condition,
    ConditionalBase,
    CountCondition,
    Evaluated,
    IfCondition,
    IfPredicate,
    CountOp
)


@dataclass(kw_only=True)
class RecordStep(Step):
    """Record preflight and execution calls."""

    events: list[str] = field(default_factory=list)

    def preflight(self) -> None:
        self.events.append("preflight")

    def do(self) -> None:
        self.events.append("do")


@dataclass(kw_only=True)
class StaticCondition(Condition):
    """Return a configured condition result."""

    result: bool

    def evaluate(self) -> bool:
        return self.result


# ----------------------------------------
# IfPredicate
# ----------------------------------------

def test_predicate_evaluate():
    step = IfPredicate(
        input_=10,
        predicate=lambda value: value > 5,
    )

    assert step.evaluate() == Evaluated(10, True)


def test_predicate_evaluate_false():
    step = IfPredicate(
        input_=3,
        predicate=lambda value: value > 5,
    )

    assert step.evaluate() == Evaluated(3, False)


def test_predicate_resolves_reference():
    reference = Reference(int, 10)

    step = IfPredicate(
        input_=reference,
        predicate=lambda value: value > 5,
    )

    assert step.evaluate() == Evaluated(10, True)


def test_predicate_evaluates_at_execution():
    reference = Reference(int, 3)

    step = IfPredicate(
        input_=reference,
        predicate=lambda value: value > 5,
    )

    assert step.evaluate().result is False

    reference.set_value(10)

    assert step.evaluate().result is True


def test_predicate_dispatches_true():
    received = []

    step = IfPredicate(
        input_=10,
        predicate=lambda value: value > 5,
        if_true=received.append,
    )

    step.do()

    assert received == [10]


def test_predicate_dispatches_false():
    received = []

    step = IfPredicate(
        input_=3,
        predicate=lambda value: value > 5,
        if_false=received.append,
    )

    step.do()

    assert received == [3]


def test_predicate_only_executes_selected_branch():
    events = []

    step = IfPredicate(
        input_=10,
        predicate=lambda value: value > 5,
        if_true=lambda value: events.append("true"),
        if_false=lambda value: events.append("false"),
    )

    step.do()

    assert events == ["true"]


# ----------------------------------------
# IfCondition
# ----------------------------------------

@pytest.mark.parametrize("result", [True, False])
def test_condition_evaluate(result: bool):
    condition = StaticCondition(result=result)

    step = IfCondition(condition=condition)

    evaluated = step.evaluate()

    assert evaluated.value is condition
    assert evaluated.result is result


@pytest.mark.parametrize(
    ("result", "expected"),
    [
        (True, "true"),
        (False, "false"),
    ],
)
def test_condition_dispatch(result: bool, expected: str):
    events = []

    condition = StaticCondition(result=result)

    step = IfCondition(
        condition=condition,
        if_true=lambda value: events.append("true"),
        if_false=lambda value: events.append("false"),
    )

    step.do()

    assert events == [expected]


def test_condition_callback_receives_condition():
    received = []
    condition = StaticCondition(result=True)

    step = IfCondition(
        condition=condition,
        if_true=received.append,
    )

    step.do()

    assert received == [condition]
    assert received[0] is condition


# ----------------------------------------
# CountCondition
# ----------------------------------------

@pytest.mark.parametrize(
    ("op", "target", "expected"),
    [
        ("==", 3, True),
        ("==", 2, False),
        ("!=", 2, True),
        ("!=", 3, False),
        (">", 2, True),
        (">", 3, False),
        (">=", 3, True),
        (">=", 4, False),
        ("<", 4, True),
        ("<", 3, False),
        ("<=", 3, True),
        ("<=", 2, False),
    ],
)
def test_count_condition(
    op: CountOp,
    target: int,
    expected: bool,
):
    condition = CountCondition(
        countable=[1, 2, 3],
        target=target,
        op=op,
    )

    assert condition.evaluate() is expected


def test_count_condition_resolves_references():
    countable = Reference(list, [1, 2, 3])
    target = Reference(int, 2)

    condition = CountCondition(
        countable=countable,
        target=target,
        op=">",
    )

    assert condition.evaluate() is True


def test_count_condition_uses_current_values():
    countable = Reference(list, [1, 2, 3])
    target = Reference(int, 2)

    condition = CountCondition(
        countable=countable,
        target=target,
        op=">",
    )

    assert condition.evaluate() is True

    target.set_value(5)

    assert condition.evaluate() is False


def test_count_condition_empty_collection():
    condition = CountCondition(
        countable=[],
        target=0,
        op="==",
    )

    assert condition.evaluate() is True


def test_count_condition_invalid_operator():
    condition = CountCondition(
        countable=[1, 2, 3],
        target=3,
        op="invalid",  # type: ignore[arg-type]
    )

    with pytest.raises(KeyError, match="invalid"):
        condition.evaluate()


# ----------------------------------------
# ConditionalBase dispatch
# ----------------------------------------

def test_dispatch_step():
    action = RecordStep()

    ConditionalBase.dispatch_action(10, action)

    assert action.events == ["do"]


def test_dispatch_callable():
    received = []

    ConditionalBase.dispatch_action(10, received.append)

    assert received == [10]


def test_dispatch_exception_instance():
    error = ValueError("Test failure")

    with pytest.raises(ValueError) as exc:
        ConditionalBase.dispatch_action(10, error)

    assert exc.value is error


def test_dispatch_exception_type():
    with pytest.raises(ValueError):
        ConditionalBase.dispatch_action(10, ValueError)


def test_dispatch_none():
    assert ConditionalBase.dispatch_action(10, None) is None


def test_dispatch_invalid_action():
    with pytest.raises(TypeError, match="Unsupported action"):
        ConditionalBase.dispatch_action(
            10,
            123,  # type: ignore[arg-type]
        )


def test_dispatch_step_action():
    action = RecordStep()

    step = IfPredicate(
        input_=10,
        predicate=lambda value: True,
        if_true=action,
    )

    step.do()

    assert action.events == ["do"]


def test_dispatch_unconfigured_branch():
    received = []

    step = IfPredicate(
        input_=10,
        predicate=lambda value: False,
        if_true=received.append,
    )

    step.do()

    assert received == []


# ----------------------------------------
# Preflight
# ----------------------------------------

def test_preflight_true_branch():
    action = RecordStep()

    step = IfPredicate(
        input_=10,
        predicate=lambda value: True,
        if_true=action,
    )

    step.preflight()

    assert action.events == ["preflight"]


def test_preflight_both_branches():
    true_action = RecordStep()
    false_action = RecordStep()

    step = IfPredicate(
        input_=10,
        predicate=lambda value: True,
        if_true=true_action,
        if_false=false_action,
    )

    step.preflight()

    assert true_action.events == ["preflight"]
    assert false_action.events == ["preflight"]


def test_preflight_requires_action():
    step = IfPredicate(
        input_=10,
        predicate=lambda value: True,
    )

    with pytest.raises(
        ValueError,
        match="At least one branch must be configured",
    ):
        step.preflight()


def test_preflight_does_not_execute_actions():
    action = RecordStep()

    step = IfPredicate(
        input_=10,
        predicate=lambda value: True,
        if_true=action,
    )

    step.preflight()

    assert action.events == ["preflight"]
