"""
Module- and file-loading to trigger the handlers to be registered.

Since the framework is based on the decorators to register the handlers,
the files/modules with these handlers should be loaded first,
thus executing the decorators.

The files/modules to be loaded are usually specified on the command-line.
Currently, two loading modes are supported, both are equivalent to Python CLI:

* Plain files files (`kopf run file.py`).
* Importable modules (`kopf run -m pkg.mod`).

Multiple files/modules can be specified. They will be loaded in the order.
"""

import importlib
import importlib.util
import os.path
import sys
from typing import Iterable


def preload(
        paths: Iterable[str],
        modules: Iterable[str],
) -> None:
    """
    Ensure the handlers are registered by loading/importing the files/modules.
    """

    for idx, path in enumerate(paths):
        sys.path.insert(0, os.path.abspath(os.path.dirname(path)))
        name = f'__kopf_script_{idx}__{path}'  # same pseudo-name as '__main__'
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)  # type: ignore

    for name in modules:
        importlib.import_module(name)
