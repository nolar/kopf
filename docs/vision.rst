======
Vision
======

Kubernetes is rising as a standard de facto for the infrastructure abstractions.

Kubernetes operators become a common way to extend Kubernetes
with the domain logic.

However, at the moment (2018-2019), the operators are mostly written in Go,
and require the advanced knowledge both of Go and of Kubernetes internals.
This makes the Kubernetes operators a skill of few, a property of the "elites".

In a perfect world of Kopf, the Kubernetes operators are a commodity,
used to build the domain logic on top of Kubernetes fast and with ease,
requiring little or no skills in the infrastructure management.

For this, Kopf hides the low-level infrastructure details from the user
(i.e. the operator developer),
exposing only the APIs and DSLs needed to express the user's domain.

In addition, Kopf does this in one of the widely used, easy to learn
programming languages: Python.

But Kopf does not go too far in abstracting the Kubernetes internals away:
it avoids the introduction of extra entities and controlling structures
(`Occam's Razor`_, `KISS`_), and most likely it will never have
a mapping of Python classes to Kubernetes resources
(like in the ORMs for the relational databases).

.. _Occam's Razor: https://en.wikipedia.org/wiki/Occam%27s_razor
.. _KISS: https://en.wikipedia.org/wiki/KISS_principle
