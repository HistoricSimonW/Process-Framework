
import pytest

from process_framework import Reference, resolve


def test_reference_initially_unset():
    ref = Reference(int)

    assert not ref.has_value()

    with pytest.raises(ValueError):
        ref.get_value()


def test_reference_initial_value():
    ref = Reference(int, 10)

    assert ref.has_value()
    assert ref.get_value() == 10


def test_reference_assignment():
    ref = Reference(int)

    ref.set_value(42)

    assert ref.has_value()
    assert ref.get_value() == 42


def test_reference_rejects_incompatible_type():
    ref = Reference(int)

    with pytest.raises(ValueError):
        ref.set_value("invalid")    # type: ignore type-arg


def test_reference_accepts_none():
    ref = Reference(int, 42)

    ref.set_value(None)

    assert not ref.has_value()


def test_resolve():
    ref = Reference(int, 42)

    assert resolve(ref) == 42
    assert resolve(42) == 42
