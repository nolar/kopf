# Bootstrap the development environment

## Minikube cluster

To develop the framework and the operators in an isolated Kubernetes cluster,
use [minikube](https://github.com/kubernetes/minikube):

MacOS:

```bash
brew install docker-machine-driver-hyperkit
sudo chown root:wheel /usr/local/opt/docker-machine-driver-hyperkit/bin/docker-machine-driver-hyperkit
sudo chmod u+s /usr/local/opt/docker-machine-driver-hyperkit/bin/docker-machine-driver-hyperkit

brew cask install minikube
minikube config set vm-driver hyperkit
```

Start the minikube cluster:

```bash
minikube start
minikube dashboard
```

It will automatically create and activate the kubectl context named `minikube`.
If not, or if you have multiple clusters, activate it explicitly:

```bash
kubectl config get-contexts
kubectl config current-context
kubectl config use-context minikube
```


## Cluster setup

Apply the framework's peering resource definition (for neighbourhood awareness):

```bash
kubectl apply -f peering.yaml
```

Apply the custom resource definitions of your application
(here, we use an example application and resource):

```bash
kubectl apply -f examples/crd.yaml
```


## Runtime setup

Install the operator to your virtualenv in the editable mode
(and all its dependencies):

```bash
pip install -e .
kopf --help
```

Run the operator in the background console/terminal tab:

```bash
kopf run examples/01-minimal/example.py --verbose
```

Create and delete a sample object (just an example here).
Observe how the operator reacts and prints the logs,
and how the handling progress is reported on the object's events.

```bash
kubectl apply -f examples/obj.yaml
kubectl describe -f examples/obj.yaml
kubectl delete -f examples/obj.yaml
```

## PyCharm & IDEs

If you use PyCharm, create a Run/Debug Configuration as follows:

* Mode: `module name`
* Module name: `kopf`
* Arguments: `run examples/01-minimal/example.py --verbose`
* Python Interpreter: anything with Python>=3.7

Stop the console operator, and start the IDE debug session.
Put a breakpoint in the used operator script on the first line of the function.
Repeat the object creation, and ensure the IDE stops at the breakpoint.

Congratulations! You are ready to develop and debug your own operator.


## Real cluster

**WARNING:** Running the operator against a real cluster can influence
the real applications in the ways not expected by other team members.
The dev-mode operator's logs will not be visible in the central loggging,
as there are not sent there. Use the real clusters only when you have
the strong reasons to do so, such as the system resource requests
(CPU, RAM, PVC), which are not achievable in the minikube's VMs.

**WARNING:** Running multiple operators for the same cluster without isolation 
can cause infinite loops, conflicting changes, and duplicated side effects
(such as the children object creation, e.g. jobs, pods, etc).
It is your responsibility to design the deployment in such a way that
the operators do not collide. The framework helps by providing the `--peering`
and `--namespace` CLI options, but does not prevent the mis-configurations.

To run against the real cluster, use the dev-mode of the framework.
This will set the operator's priority to 666 (just a high number),
and will freeze all other running operators (the default priority is 0)
for the runtime, so that they do not collide with each other:

```bash
kopf run examples/01-minimal/example.py --verbose --dev
```

Alternatively, explicitly freeze/resume all other operators,
and it will freeze them even if your operator is not running
(e.g., for 2 hours):

```bash
kopf freeze --lifetime $((2*60*60))
kopf resume
```


## Cleanup

To cleanup the cluster from the framework-related objects:

```bash
kubectl delete -f peering.yaml
kubectl delete -f examples/obj.yaml
kubectl delete -f examples/crd.yaml
```

For the minikube cleanup (to release the CPU/RAM/disk resources):

```bash
minikube stop
minikube delete
```
