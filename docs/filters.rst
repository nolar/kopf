=========
Filtering
=========

.. highlight:: python

It is possible to only execute handlers when the object that triggers a handler
matches certain criteria.

The following filters are available for all resource-related handlers
(event-watching and change-detecting):


By labels
=========

* Match an object's label and value::

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples',
                    labels={'some-label': 'somevalue'})
    def my_handler(spec, **_):
        pass

* Match on the existence of an object's label::

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples',
                    labels={'some-label': kopf.PRESENT})
    def my_handler(spec, **_):
        pass

* Match on the absence of an object's label::

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples',
                    labels={'some-label': kopf.ABSENT})
    def my_handler(spec, **_):
        pass


By annotations
==============

* Match on object's annotation and value::

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples',
                    annotations={'some-annotation': 'somevalue'})
    def my_handler(spec, **_):
        pass

* Match on the existence of an object's annotation::

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples',
                    annotations={'some-annotation': kopf.PRESENT})
    def my_handler(spec, **_):
        pass

* Match on the absence of an object's annotation::

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples',
                    annotations={'some-annotation': kopf.ABSENT})
    def my_handler(spec, **_):
        pass


By arbitrary callbacks
======================

* Check on any field on the body with a when callback.
  The filter callback takes the same args as a handler (see :doc:`kwargs`)::

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples',
                    when=lambda spec, **_: spec.get('my-field') == 'somevalue')
    def my_handler(spec, **_):
        pass

* Check on labels/annotations with an arbitrary callback for individual values
  (the value comes as the first positional argument, plus usual :doc:`kwargs`)::

    def check_value(value, spec, **_):
        return value == 'some-value' and spec.get('field') is not None

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples',
                    labels={'some-label': check_value},
                    annotations={'some-annotation': check_value})
    def my_handler(spec, **_):
        pass
