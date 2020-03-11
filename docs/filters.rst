=========
Filtering
=========

.. highlight:: python

It is possible to only execute handlers when the object that triggers a handler
matches certain filters.

The following filters are available for all event, cause, and field handlers:

* Match an object's label and value::

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples', labels={'somelabel': 'somevalue'})
    def my_handler(spec, **_):
        pass

* Match on the existence of an object's label::

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples', labels={'somelabel': None})
    def my_handler(spec, **_):
        pass

* Match an object's annotation and value::

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples', annotations={'someannotation': 'somevalue'})
    def my_handler(spec, **_):
        pass

* Match on the existence of an object's annotation::

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples', annotations={'someannotation': None})
    def my_handler(spec, **_):
        pass

* Check on any field on the body with a when callback. The filter callback takes the same args as a handler::

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples', when=lambda body, **_: body.get('spec', {}).get('myfield', '') == 'somevalue')
    def my_handler(spec, **_):
        pass
