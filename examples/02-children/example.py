import kopf
import kubernetes.client
import yaml


@kopf.on.create('zalando.org', 'v1', 'kopfexamples')
def create_fn(body, spec, **kwargs):

    # Render the pod yaml with some spec fields used in the template.
    doc = yaml.load(f"""
        apiVersion: v1
        kind: Pod
        spec:
          containers:
          - name: the-only-one
            image: busybox
            command: ["sh", "-x", "-c"]
            args: 
            - |
              echo "FIELD=$FIELD"
              sleep {spec.get('duration', 0)}
            env:
            - name: FIELD
              value: {spec.get('field', 'default-value')}
    """)

    # Make it our child: assign the namespace, name, labels, owner references, etc.
    kopf.adopt(doc, owner=body)

    # Actually create an object by requesting the Kubernetes API.
    api = kubernetes.client.CoreV1Api()
    pod = api.create_namespaced_pod(namespace=doc['metadata']['namespace'], body=doc)

    # Update the parent's status.
    return {'children': [pod.metadata.uid]}
