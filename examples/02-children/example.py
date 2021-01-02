import kopf
import pykube
import yaml


@kopf.on.create('kopfexamples')
def create_fn(spec, **kwargs):

    # Render the pod yaml with some spec fields used in the template.
    doc = yaml.safe_load(f"""
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
    kopf.adopt(doc)

    # Actually create an object by requesting the Kubernetes API.
    api = pykube.HTTPClient(pykube.KubeConfig.from_env())
    pod = pykube.Pod(api, doc)
    pod.create()
    api.session.close()

    # Update the parent's status.
    return {'children': [pod.metadata['uid']]}
