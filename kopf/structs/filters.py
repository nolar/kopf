def match(handler, body, changed_fields=None):
    return (
        (not handler.field or _matches_field(handler, changed_fields or [])) and
        (not handler.labels or _matches_labels(handler, body)) and
        (not handler.annotations or _matches_annotations(handler, body))
    )


def _matches_field(handler, changed_fields):
    return any(field[:len(handler.field)] == handler.field for field in changed_fields)


def _matches_labels(handler, body):
    return _matches_metadata(handler=handler, body=body, metadata_type='labels')


def _matches_annotations(handler, body):
    return _matches_metadata(handler=handler, body=body, metadata_type='annotations')


def _matches_metadata(handler, body, metadata_type):
    metadata = getattr(handler, metadata_type)
    object_metadata = body.get('metadata', {}).get(metadata_type, {})

    for key, value in metadata.items():
        if key not in object_metadata:
            return False
        elif value is not None and value != object_metadata[key]:
            return False
        else:
            continue

    return True
