"""
A contract for the import linter to secure 3rd-party clients importing.

Wrong:

.. code-block:: python

    import kubernetes

Right:

.. code-block:: python

    try:
        import kubernetes
    except ImportError:
        ...

https://import-linter.readthedocs.io/en/stable/custom_contract_types.html
"""
import ast
import os.path

from grimp import ImportGraph
from importlinter import Contract, ContractCheck, fields, output


class ConditionalImportContract(Contract):
    """
    Contract that defines a single forbidden import between
    two modules.
    """
    source_modules = fields.ListField(subfield=fields.ModuleField())
    conditional_modules = fields.ListField(subfield=fields.ModuleField())

    def check(self, graph: ImportGraph, verbose: bool) -> ContractCheck:
        failed_details = []

        # Combine all source x all target (secured) modules.
        conditional_modules = [m for m in self.conditional_modules if m.name in graph.modules]
        for source_module in self.source_modules:
            for conditional_module in conditional_modules:

                # For every pair of source & target, find all import chains.
                chains = graph.find_shortest_chains(
                    importer=source_module.name,
                    imported=conditional_module.name,
                )
                for chain in chains:
                    # Of each chain, we only need the tail for our analysis.
                    # A sample chain: ('kopf.on', 'kopf._core.intents.registries', 'pykube')
                    importer, imported = chain[-2:]
                    details = graph.get_import_details(
                        importer=importer,
                        imported=imported
                    )

                    # For each import (possible several per file), get its line number and check it.
                    for detail in details:
                        ok = self._check_secured_import(detail['importer'], detail['line_number'])
                        if not ok:
                            failed_details.append(detail)

        return ContractCheck(
            kept=not failed_details,
            metadata={'failed_details': failed_details},
        )

    def render_broken_contract(self, check):
        for detail in check.metadata['failed_details']:
            importer = detail['importer']
            imported = detail['imported']
            line_number = detail['line_number']
            line_contents = detail['line_contents']
            output.print_error(
                f'{importer} is not allowed to import {imported} without try-except-ImportError:',
                bold=True,
            )
            output.new_line()
            output.indent_cursor()
            output.print_error(f'{importer}:{line_number}: {line_contents}')

    def _check_secured_import(self, mod: str, lno: int) -> bool:
        """
        True if the specified import statement suppresses the ``ImportError``.
        False if unsecured or secured with some other unrelated error handling.
        """

        # Some hard-coded heuristics because importlib fails on circular imports.
        # TODO: switch to: importlib.util.find_spec(mod)?.origin
        path = os.path.join(os.path.dirname(__file__), mod.replace('.', '/')) + '.py'
        with open(path, encoding='utf-8') as f:
            text = f.read()
            root = ast.parse(text)

        # For any candidate "try-except-ImportError" clause, check if it of interest for us,
        # i.e. import the specified module inside. Check in all levels of nestedness.
        for node in ast.walk(root):
            match node:
                case ast.Try():
                    suppresses_import_errors = any(
                        isinstance(excpt.type, ast.Name) and excpt.type.id == 'ImportError'
                        for excpt in node.handlers
                    )
                    contains_that_import = any(
                        isinstance(expr, ast.Import | ast.ImportFrom) and expr.lineno == lno
                        for expr in node.body
                    )
                    if contains_that_import and suppresses_import_errors:
                        return True  # ok, secured

        # 1) no try-except blocks at all, at any depth level, so the import goes naked.
        # 2) there are try-except blocks for that import, but not with importing errors.
        # 3) there are try-except blocks for importing errors, but for other unrelated imports.
        return False  # not ok, unsecured
