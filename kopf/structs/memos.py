from typing import Any, Dict


class Memo(Dict[Any, Any]):
    """ A container to hold arbitrary keys-fields assigned by the users. """

    def __setattr__(self, key: str, value: Any) -> None:
        self[key] = value

    def __delattr__(self, key: str) -> None:
        try:
            del self[key]
        except KeyError as e:
            raise AttributeError(str(e))

    def __getattr__(self, key: str) -> Any:
        try:
            return self[key]
        except KeyError as e:
            raise AttributeError(str(e))
