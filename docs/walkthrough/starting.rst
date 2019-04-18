=====================
Starting the operator
=====================

Previously, we have defined a :doc:`problem <problem>` that we are solving,
and created the :doc:`custom resource definitions <resources>`
for the ephemeral volume claims.

Now, we are ready to write some logic for this kind of objects.
Let's start with the an operator skeleton that does nothing useful --
just to see how it can be started.

.. code-block:: python
   :name: skeleton
   :linenos:
   :caption: ephemeral.py

    import kopf

    @kopf.on.create('zalando.org', 'v1', 'ephemeralvolumeclaims')
    def create_fn(body, **kwargs):
        print(f"A handler is called with body: {body}")

.. note::
    Despite an obvious desire, do not name the file as ``operator.py``,
    since there is a built-in module in Python 3 with this name,
    and there could be potential conflicts on the imports.

Let's run the operator and see what will happen:

.. code-block:: bash

    kopf run ephemeral.py --verbose


The output looks like this:

.. code-block:: none

    ... TODO...

.. todo:: add the outpit sample^^

Note that the operator has noticed an object created before the operator
was even started, and handled it -- since it was not handled before.

Now, you can stop the operator with Ctrl-C (twice), and start it again:

.. code-block:: bash

    kopf run ephemeral.py --verbose

The operator will not handle the object, as now it is already successfully
handled. This is important in case of the operator is restarted if it runs
in a normally deployed pod, or when you restart the operator for debugging.

Let's delete and re-create the same object to see the operator reacting:

.. code-block:: bash

    kubectl delete -f obj.yaml
    kubectl apply -f obj.yaml

