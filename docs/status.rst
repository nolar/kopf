==================
Development Status
==================

Kopf is production-ready and stable (semantic v1).
Major bugs are fixed ASAP (there were none for a long time).
Minor bugs are fixed as time and energy permit, or a workaround is provided.

There is no active development of **new major** functionality for Kopf --- the whole idea of a framework for operators is fully expressed and implemented, I have nothing more to add. This piece of art is finished. (This might change.)

Minor feature requests can be implemented from time to time.
Maintenance for new versions of Python and Kubernetes is performed regularly.
Some internal optimizations are planned, such as a reduced memory footprint, high-load readiness, or agentic friendliness --- but they will be backwards-compatible (no semantic v2 with breaking changes on the horizon).

All PRs are carefully and thoughtfully reviewed by an organic maintainer in order to minimize the risk of supply chain attacks. Beware: sub-dependencies, so as the maintainer's devices and accounts can still become compromised under certain circumstances, so the risk is not entirely eliminated. GHA CI uploads the full provenance to PyPI for validation of the source. **Do your due diligence on every upgrade.**
