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


def preload(paths, modules):
    """
    Ensure the handlers are registered by loading/importing the files/modules.
    """

    for path in paths:
        name, _ = os.path.splitext(os.path.basename(path))
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

    for name in modules:
        importlib.import_module(name)
