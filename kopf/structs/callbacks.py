"""
Callback signatures for typing.

Since these signatures contain a lot of copy-pasted kwargs and are
not so important for the codebase, they are moved to this separate module.
"""
import logging
from typing import NewType, Any, Union, Optional

from typing_extensions import Protocol

from kopf.structs import bodies
from kopf.structs import diffs
from kopf.structs import patches

# A specialised type to highlight the purpose or origin of the data of type Any,
# to not be mixed with other arbitrary Any values, where it is indeed "any".
Result = NewType('Result', object)


class ActivityHandlerFn(Protocol):
    def __call__(  # lgtm[py/similar-function]
            self,
            *args: Any,
            logger: Union[logging.Logger, logging.LoggerAdapter],
            **kwargs: Any,
    ) -> Optional[Result]: ...


class ResourceHandlerFn(Protocol):
    def __call__(  # lgtm[py/similar-function]
            self,
            *args: Any,
            type: str,
            event: Union[str, bodies.RawEvent],
            body: bodies.Body,
            meta: bodies.Meta,
            spec: bodies.Spec,
            status: bodies.Status,
            uid: str,
            name: str,
            namespace: Optional[str],
            patch: patches.Patch,
            logger: Union[logging.Logger, logging.LoggerAdapter],
            diff: diffs.Diff,
            old: Optional[Union[bodies.BodyEssence, Any]],  # "Any" is for field-handlers.
            new: Optional[Union[bodies.BodyEssence, Any]],  # "Any" is for field-handlers.
            **kwargs: Any,
    ) -> Optional[Result]: ...


class WhenFilterFn(Protocol):
    def __call__(  # lgtm[py/similar-function]
            self,
            *args: Any,
            type: str,
            event: Union[str, bodies.RawEvent],
            body: bodies.Body,
            meta: bodies.Meta,
            spec: bodies.Spec,
            status: bodies.Status,
            uid: str,
            name: str,
            namespace: Optional[str],
            patch: patches.Patch,
            logger: Union[logging.Logger, logging.LoggerAdapter],
            diff: diffs.Diff,
            old: Optional[Union[bodies.BodyEssence, Any]],  # "Any" is for field-handlers.
            new: Optional[Union[bodies.BodyEssence, Any]],  # "Any" is for field-handlers.
            **kwargs: Any,
    ) -> bool: ...
