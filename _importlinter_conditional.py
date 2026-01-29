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
import os.path

import astpath
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

        # Some hard-coded heuristics because importlib fails on circular imports.
        # TODO: switch to: importlib.util.find_spec(mod)?.origin
        path = os.path.join(os.path.dirname(__file__), mod.replace('.', '/')) + '.py'
        with open(path, encoding='utf-8') as f:
            text = f.read()
            xtree = astpath.file_contents_to_xml_ast(text)

        # For every "import" of interest, find any surrounding "try-except-ImportError" clauses.
        for node in xtree.xpath(f'''//Import[@lineno={lno!r}]'''):
            tries = node.xpath('''../parent::Try[//ExceptHandler/type/Name/@id="ImportError"]''')
            if not tries:
                return False
        return True
