from typing import Callable, Generic, Optional, Type, TypeVar

T_OUT = TypeVar("T_OUT")
T_DECO = TypeVar("T_DECO")

# TODO: finish typing, bound T_DECO?


class classproperty(Generic[T_OUT]):
    """Class/instance property decorator"""

    def __init__(self, method: Callable[[T_DECO], T_OUT]):
        self.fget = method

    def __get__(self, instance: Optional[T_DECO], cls: Type[T_DECO]) -> T_OUT:
        return self.fget(instance or cls)  # type: ignore[misc, arg-type]
