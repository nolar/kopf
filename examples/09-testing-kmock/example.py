from typing import Any

import kopf
import pykube


# That is in the operator that we are going to test.
@kopf.on.delete('kopf.dev', 'v1', 'kopfexamples')
def delete_children_pods(namespace: str | None, name: str | None, **_: Any) -> None:
    api = pykube.HTTPClient(pykube.KubeConfig.from_env())
    pykube.Pod.objects(api).filter(namespace=namespace).get_by_name('pod1').delete()
