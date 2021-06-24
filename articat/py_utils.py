import functools
import inspect
from typing import Callable, TypeVar

T = TypeVar("T")


def lazy(fn: Callable[[], T]) -> Callable[[], T]:
    """
    lazy "object", useful to delay computation. inspired by scala's lazy.

    Example:

    ```
    x = lazy(lambda: heavy_expensive_object())
    ...
    x().do_sth()
    x().do_sth()
    ```

    Note that you need to call the lazy object to retrieve the underlying
    object. Also lazy is not thread safe because it depends on functools.lru_cache
    which is also not thread safe.
    """
    if inspect.isfunction(fn) or inspect.ismethod(fn):
        fn_spec = inspect.signature(fn)
        # can only be a "function" without any arguments
        if len(fn_spec.parameters) > 0:
            raise TypeError("Can't annotate a function with arguments")

    return functools.lru_cache(1)(fn)
