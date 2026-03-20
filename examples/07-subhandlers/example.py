from typing import Any

import kopf


@kopf.on.create('kopfexamples')
def create_fn(spec: kopf.Spec, **_: Any) -> None:

    for item in spec.get('items', []):

        @kopf.subhandler(id=item)
        async def create_item_fn(item: str = item, **_: Any) -> None:
            print(f"=== Handling creation for {item}. ===")
