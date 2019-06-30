"""

"""

def has_filter(handler):
    return bool(handler.field or handler.labels or handler.annotations)


def matches_filter(handler, body, changed_fields=None):
    changed_fields = changed_fields or {}
    
    for key, fn in _FILTERS.items():
        if getattr(handler, key) and not fn(handler, body, changed_fields):
            return False
            
    return True


def _matches_field(handler, changed_fields):
    return any(field[:len(handler.field)] == handler.field for field in changed_fields)


def _matches_metadata(handler, body, metadata_type):
    metadata = getattr(handler, metadata_type)
    object_metadata = body.get('metadata', {}).get(metadata_type, {})

    if not metadata:
        return True

    for key, value in metadata.items():
        if key not in object_metadata:
            return False
        elif value is not None and value != object_metadata[key]:
            return False
        else:
            continue
    
    return True


_FILTERS = {
    'field': (lambda handler, _, changed_fields: _matches_field(handler, changed_fields)),
    'labels': (lambda handler, body, _: _matches_metadata(handler, body, metadata_type='labels')),
    'annotations': (lambda handler, body, _: _matches_metadata(handler, body, metadata_type='annotations')),
}