=========
Critiques
=========

  Critique is a constructive, detailed analysis aimed at improvement, focusing on both strengths and weaknesses. Conversely, criticism is often subjective, judgmental, and focused on finding faults, typically aimed at disapproval. While a critique encourages growth, criticism is often destructive. /Google on "critique vs. criticism"/

Kopf has several known flaws on the design level. Here they are listed and addressed to help you make the best decision on how to build your operator.


Python is slow and resource-greedy
==================================

Python is an interpreted language. In particular, among many other things, this implies it consumes lots of memory at runtime and is overall slower than the compiled languages like Go. Kubernetes world uses Go as the primary language, so this is a good and fair baseline for comparison for the performance of operators.

Some reports (source lost) say that rewriting an operator from Python to Go reduced the memory usage from ≈300 MB to ≈30 MB, i.e. ≈10x. All these concerns are valid, and if memory usage is of critical concern, you should use Go indeed.

Internal measurements using ``memray`` and ``memory_profiler`` show that the operator's data structures consume barely any memory. With a no-op operator (no meaningful domain logic; no Kubernetes API clients installed) and 1'000 resources handled in an artificial setup, roughly ≈50% of memory went to Python modules imported, and ≈30% were Python's TCP connections and SSL contexts for the API calls; only ≈20% was the operator runtime. With the ``kubernetes`` official client installed, the modules took ≈75% of memory (the client alone took ≈60%), ≈15-20% went to TCP/SSL, and ≈10% to the operator. Switching from CPython to PyPy (which is officially supported by Kopf) gave no benefit.

To the best of my knowledge, nothing can be done in this regard. This issue is the focus of the Python community worldwide, people are making efforts to improve Python's performance here and there.

However, Python is known not for its runtime performance, but for the expressive power and the ease of use, especially at early stages of any product. It is a language for quick prototyping and for making minimally viable projects and products fast. And since it is one of the most used languages, there is apparently a demand for simplicity.

Kopf sticks to the same paradigm of quick-starting the operators, writing small operators for ad-hoc tasks. Kopf's roadmap contains some tasks to improve the performance of Kopf and prepare it for high-load tasks and large clusters, but it will never be as good as Go-based operators in terms of resource usage --- that is not the goal. Expressive power remains the primary value and the main goal.


Level-based vs. edge-based triggering
=====================================

`Another critique`__ claims that Kopf suggests some bad practices in terms of handling resources: edge-based triggering instead of level-based triggering.

__ https://www.reddit.com/r/kubernetes/comments/1dge5qk/comment/l8s3n3i/

It is easier to explain with an example:

Imagine you have an operator with a resource containing the ``spec.replicas`` field, which is originally 3. You then change it to 2, then back to 3, while you have only 1 replica running at the moment.

In level-based triggering, the change of 3-2-3 should not be of any concern to the operator at all. What matters is that you have 1 replica running, so the operator's effort should be on bringing the actual state (1) to the desired state (2 or 3), so adding +2 more replicas.

Kopf, on the other hand, will be focusing on the change (edge-based triggering) from 3 to 2 and back to 3 until it is applied. Kopf does not have any understanding of the "actual state" as the baseline for comparison, unless the operator's developer implements it.

This is a fair critique, and I, as the author, fully admit this flaw. Nevertheless, I would like to address it with a counter-argument.

Kopf's low-level handlers (on-event, indexes, daemons/timers), i.e. those that do not track the state, are the direct equivalents of Go/Kubernetes concepts of operators in level-based triggering. If you limit yourselves only to those, you can implement any reconciliation technique or any state machine similar to Go-based operators.

Kopf's high-level handlers (on-creation/update/deletion), i.e. those that do track the state and calculate the diffs of changes, are intended for ease of use with ad-hoc operators. This is an addon on top of what is available in Go/Kubernetes frameworks, in line with the stated goal of making it easy to express and prototype the ad-hoc operators from scratch.

Indeed, the mere existence of these high-level handlers suggests and provokes the "bad practices" of event-driven operator design and edge-based triggering. But the decision always lies with the developers of the actual operators and which trade-offs they are willing or not willing to make.

Developers aiming for high-grade Kubernetes-native operators designed with the best practices of level-based triggering and reconciliation should learn the subtle differences between these concepts (edge- vs. level-based triggering), and design the operators accordingly from the very beginning. See :doc:`reconciliation` for an example of level-based triggering with a calculated "actual state".

Kopf will be supporting both ways indefinitely, in line with its goals (ease of use, quick prototyping, ad-hoc solutions). Some better ways of reconciliation might appear later, which will connect an "actual state" (calculated) to a "desired state" (usually, ``spec``). There are no specific timelines or plans.
