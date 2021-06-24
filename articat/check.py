from typing import Any, Type, TypeVar

T = TypeVar("T")


class CheckException(Exception):
    pass


def check_type(o: Any, type: Type[T]) -> T:
    """Asserts type and returns the object"""
    if not isinstance(o, type):
        raise CheckException(f"{o} is not of type {type}")
    return o
