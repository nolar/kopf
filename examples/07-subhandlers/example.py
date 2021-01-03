import kopf


@kopf.on.create('kopfexamples')
def create_fn(spec, **kwargs):

    for item in spec.get('items', []):

        @kopf.subhandler(id=item)
        async def create_item_fn(item=item, **kwargs):
            print(f"=== Handling creation for {item}. ===")
