=========
Critiques
=========

  Critique is a constructive, detailed analysis aimed at improvement, focusing on both strengths and weaknesses. Conversely, criticism is often subjective, judgmental, and focused on finding faults, typically aimed at disapproval. While a critique encourages growth, criticism is often destructive. /Google on "critique vs. criticism"/

Kopf has several known design-level flaws. They are listed and addressed here
to help you make the best decision on how to build your operator.


Python is slow and resource-greedy
==================================

Python is an interpreted language. Among other things, this means it consumes
more memory at runtime and is overall slower than compiled languages such as Go.
The Kubernetes ecosystem uses Go as its primary language, making it a natural
and fair baseline for comparing operator performance.

Some reports (source lost) say that rewriting an operator from Python to Go
reduced memory usage from ≈300 MB to ≈30 MB, i.e. ≈10x.
These concerns are valid, and if memory usage is a critical concern,
you should indeed use Go.

Internal measurements using ``memray`` and ``memory_profiler`` show that
the operator's data structures consume barely any memory. With a no-op operator
(no meaningful domain logic; no Kubernetes API clients installed)
and 1,000 resources handled in an artificial setup, roughly ≈50% of memory
went to imported Python modules, and ≈30% went to Python's TCP connections
and SSL contexts for the API calls; only ≈20% was the operator runtime.
With the official ``kubernetes`` client installed, modules took ≈75% of memory
(the client alone took ≈60%), ≈15–20% went to TCP/SSL, and ≈10% to the operator.
Switching from CPython to PyPy (which is officially supported by Kopf) gave no benefit.

To the best of my knowledge, nothing can be done about this. It is a focus
of the Python community worldwide, and people are making efforts to improve
Python's performance on many fronts.

However, Python is known not for its runtime performance but for its
expressive power and ease of use, especially in the early stages of a product.
It is a language for quick prototyping and for building minimally viable products rapidly.
And since it is one of the most widely used languages, there is clearly demand for that simplicity.

Kopf follows the same paradigm: quickly starting operators and writing
small operators for ad-hoc tasks. Kopf's roadmap includes work to improve
performance and prepare for high-load tasks and large clusters,
but it will never match Go-based operators in terms of resource usage ---
that is not the goal. Expressive power remains the primary value and the main goal.


Level-based vs. edge-based triggering
=====================================

`Another critique`__ claims that Kopf suggests some bad practices in terms
of handling resources: edge-based triggering instead of level-based triggering.

__ https://www.reddit.com/r/kubernetes/comments/1dge5qk/comment/l8s3n3i/

It is easier to explain with an example.

Imagine you have an operator with a resource containing the ``spec.replicas`` field,
which is originally 3. You then change it to 2, then back to 3, while only 1 replica
is actually running at the moment.

In level-based triggering, the change sequence 3→2→3 should not concern the operator at all.
What matters is that you have 1 replica running, so the operator's effort
should go toward bringing the actual state (1) to the desired state (2 or 3),
i.e. adding 2 more replicas.

Kopf, on the other hand, focuses on the change (edge-based triggering)
from 3 to 2 and back to 3 until it is applied.
Kopf has no concept of "actual state" as a baseline for comparison,
unless the operator developer implements it.

This is a fair critique, and I, as the author, fully admit this flaw.
Nevertheless, I would like to offer a counter-argument.

Kopf's low-level handlers (on-event, indexes, daemons/timers)
--- those that do not track state --- are direct equivalents
of the Go/Kubernetes operator concepts used in level-based triggering.
If you limit yourself to those, you can implement any reconciliation technique
or state machine similar to Go-based operators.

Kopf's high-level handlers (on-creation/update/deletion)
--- those that do track state and calculate diffs ---
are intended for ease of use with ad-hoc operators.
They are an add-on on top of what is available in Go/Kubernetes frameworks,
in line with the stated goal of making it easy to express and prototype
ad-hoc operators from scratch.

Indeed, the mere existence of these high-level handlers suggests and encourages
the "bad practices" of event-driven operator design and edge-based triggering.
But the decision always rests with the developers of the actual operators
and which trade-offs they are willing or unwilling to make.

Developers aiming for high-grade Kubernetes-native operators built
on the best practices of level-based triggering and reconciliation
should learn the subtle differences between these concepts
(edge- vs. level-based triggering) and design their operators accordingly
from the very beginning. See :doc:`reconciliation` for an example
of level-based triggering with a calculated "actual state".

Kopf will support both approaches indefinitely, in line with its goals
(ease of use, quick prototyping, ad-hoc solutions).
Better reconciliation approaches may emerge later, connecting an "actual state"
(calculated) to a "desired state" (usually ``spec``).
There are no specific timelines or plans.
