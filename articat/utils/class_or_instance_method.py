from __future__ import annotations

from functools import partial, update_wrapper
from typing import Callable, Generic, Optional, Type, TypeVar

T_OUT = TypeVar("T_OUT")
T_DECO = TypeVar("T_DECO")


class class_or_instance_method(Generic[T_OUT]):
    """
    Instance/class method decorator. If available uses the instance,
    if called on class uses the class.

    Note: you can't easily implement a reliable "class property",
    without running into edge cases that will evaluate the property,
    for example help(klass).

    Example:

        class Foo:
            _bar: int = 42

            def __init__(self):
                self._bar = 128

            @class_or_instance_method
            def bar(self_or_cls):
                return self_or_cls._bar

        Foo.bar() #42
        Foo().bar() #128
    """

    def __init__(self, method: Callable[[T_DECO], T_OUT]):
        # NOTE: method appears unbound, so we don't need to
        #       worry about weakref here.
        self.org_method = method

    def __get__(
        self, instance: Optional[T_DECO], cls: Type[T_DECO]
    ) -> Callable[[], T_OUT]:
        self_or_cls = instance or cls
        wrapper = partial(self.org_method, self_or_cls)
        return update_wrapper(wrapper, self.org_method)
