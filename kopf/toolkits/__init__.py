"""
Toolkits to improve the developer experience in the context of Kopf.

They are not needed to use the framework or to run the operator
(unlike all other packages), but they can make the development
of the operators much easier.

Some things can be considered as the clients' responsibilities
rather than the operator framework's responsibilities.
In that case, the decision point is whether the functions work
"in the context of Kopf" at least to some extent
(e.g. by using its contextual information of the current handler).
"""
