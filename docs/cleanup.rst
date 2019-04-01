=======
Cleanup
=======

Once we have finished with the experiments, let's cleanup the cluster
(or you can just reset it if it is Minikube):

.. code-block:: bash

    kubectl delete -f obj.yaml
    kubectl delete -f crd.yaml
