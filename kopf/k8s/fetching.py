import functools

import kubernetes


def list_objs(*, resource, namespace=None):
    """
    List the objects of specific resource type.

    The cluster-scoped call is used in two cases:

    * The resource itself is cluster-scoped, and namespacing makes not sense.
    * The operator serves all namespaces for the namespaced custom resource.

    Otherwise, the namespace-scoped call is used:

    * The resource is namespace-scoped AND operator is namespaced-restricted.
    """
    api = kubernetes.client.CustomObjectsApi()
    if namespace is None:
        rsp = api.list_cluster_custom_object(
            group=resource.group,
            version=resource.version,
            plural=resource.plural,
        )
    else:
        rsp = api.list_namespaced_custom_object(
            group=resource.group,
            version=resource.version,
            plural=resource.plural,
            namespace=namespace,
        )
    return rsp


def make_list_fn(*, resource, namespace=None):
    """
    Return a function to be called to receive the list of objects.
    Needed in that form for the API streaming calls (see watching.py).

    However, the returned function is already bound to the specified
    resource type, and requires no resource-identifying parameters.

    Docstrings are important! Kubernetes client uses them to guess
    the returned object types and the parameters type.
    Function wrapping does that: preserves the docstrings.
    """
    api = kubernetes.client.CustomObjectsApi()
    if namespace is None:
        @functools.wraps(api.list_cluster_custom_object)
        def list_fn(**kwargs):
            return api.list_cluster_custom_object(
                group=resource.group,
                version=resource.version,
                plural=resource.plural,
                **kwargs)
    else:
        @functools.wraps(api.list_cluster_custom_object)
        def list_fn(**kwargs):
            return api.list_namespaced_custom_object(
                group=resource.group,
                version=resource.version,
                plural=resource.plural,
                namespace=namespace,
                **kwargs)
    return list_fn
