=======
Cleanup
=======

To cleanup the cluster after all the experiments are finished:

.. code-block:: bash

    kubectl delete -f obj.yaml
    kubectl delete -f crd.yaml

Alternatively, Minikube can be reset for the full cluster cleanup.
