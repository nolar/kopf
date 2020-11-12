=======
Updating Status
=======

Kopf will take the return value of all state-changing handlers (even
sub-handlers) and add them to the ``Status`` of the corresponding Kuberenetes
resource.  By default, this is stored under the handler id which by default is 
the function name.

.. note::

    The ``CustomResourceDefinition`` requires that the fields that will be set in
    status have a corresponding schema.

    You could also use ``x-kubernetes-preserve-unknown-fields: true``::

      schema:
        openAPIV3Schema:
          type: object
          properties:
            status:
              type: object
              x-kubernetes-preserve-unknown-fields: true
            spec:
              ...

Given the following handler definition::

    import kopf

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples')
    def my_handler(spec, **_):
        return {'field': 'value}

The resulting object will have the following data::

    spec:
      ...
    status:
      my_handler:
        field: value

In order to remove the handler ID from the result, you may set the optional
``status_prefix=False`` when defining the handler.

So with the handler definition::

    import kopf

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples', status_prefix=False)
    def my_handler(spec, **_):
        return {'field': 'value}

The resulting object will have the following data::

    spec:
      ...
    status:
      field: value
