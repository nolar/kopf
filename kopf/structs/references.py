import asyncio
import dataclasses
import enum
import fnmatch
import re
import urllib.parse
from typing import Collection, FrozenSet, Iterable, Iterator, List, Mapping, \
                   MutableMapping, NewType, Optional, Pattern, Set, Union

# A namespace specification with globs, negations, and some minimal syntax; see `match_namespace()`.
# Regexps are also supported if pre-compiled from the code, not from the CLI options as raw strings.
NamespacePattern = Union[str, Pattern]

# A specific really existing addressable namespace (at least, the one assumed to be so).
# Made as a NewType for stricter type-checking to avoid collisions with patterns and other strings.
NamespaceName = NewType('NamespaceName', str)

# A namespace reference usable in the API calls. `None` means cluster-wide API calls.
Namespace = Optional[NamespaceName]


def select_specific_namespaces(patterns: Iterable[NamespacePattern]) -> Collection[NamespaceName]:
    """
    Select the namespace specifications that can be used as direct namespaces.

    It is used in a fallback scenario when the namespace observation is either
    disabled or not possible due to restricted permission, while the normal
    operation is still possible in the very specific configured namespaces.
    """
    return {
        NamespaceName(pattern)
        for pattern in patterns
        if isinstance(pattern, str)  # excl. regexps & etc.
        if not('!' in pattern or '*' in pattern or '?' in pattern or ',' in pattern)
    }


def match_namespace(name: NamespaceName, pattern: NamespacePattern) -> bool:
    """
    Check if the specific namespace matches a namespace specification.

    Each individual namespace pattern is a string that follows some syntax:

    * the pattern consists of comma-separated parts (spaces are ignored);
    * each part is either an inclusive or an exclusive (negating) glob;
    * each glob can have ``*`` and ``?`` placeholders for any or one symbols;
    * the exclusive globs start with ``!``;
    * if the the first glob is exclusive, then a preceding catch-all is implied.

    A check of whether a namespace matches the individual pattern, is done by
    iterating the pattern's globs left-to-right: the exclusive patterns exclude
    it from the match; the first inclusive pattern does the initial match, while
    the following inclusive patterns only re-match it if it was excluded before;
    i.e., they do not do the full initial match.

    For example, the pattern ``"myapp-*, !*-pr-*, *pr-123"``
    will match ``myapp-test``, ``myapp-live``, even ``myapp-pr-123``,
    but not ``myapp-pr-456`` and certainly not ``otherapp-pr-123``.
    The latter one, despite it matches the last glob, is not included
    because it was not matched by the initial pattern.

    On the other hand, the pattern ``"!*-pr-*, *pr-123"``
    (equivalent to ``"*, !*-pr-*, *pr-123"``) will match ``myapp-test``,
    ``myapp-live``, ``myapp-pr-123``, ``anyapp-anything``,
    and even ``otherapp-pr-123`` -- though not ``myapp-pr-456``.
    Unlike in the first example, the otherapp's namespace was included initially
    by the first glob (the implied ``*``), and therefore could be re-matched
    by the last glob ``*pr-123`` after being excluded by ``!*-pr-*``.

    While these are theoretical capabilities of this pattern-matching algorithm,
    it is not expected that they will be abused too much. The main intention is
    to have simple one-glob patterns (either inclusive or exclusive),
    only rarely followed by a single negation.
    """

    # Regexps are powerful enough on their own -- we do not parse or interpret them.
    if isinstance(pattern, re.Pattern):
        return bool(pattern.fullmatch(name))

    # The first pattern should be an inclusive one. Unless it is, prepend a catch-all pattern.
    globs = [glob.strip() for glob in pattern.split(',')]
    if not globs or globs[0].startswith('!'):
        globs.insert(0, '*')

    # Iterate and calculate: every inclusive pattern makes the namespace to match regardless,
    # of the previous result; every exclusive pattern un-matches it if it was matched before.
    matches = first_match = fnmatch.fnmatch(name, globs[0])
    for glob in globs[1:]:
        if glob.startswith('!'):
            matches = matches and not fnmatch.fnmatch(name, glob.lstrip('!'))
        else:
            matches = matches or (first_match and fnmatch.fnmatch(name, glob))

    return matches


# Detect conventional API versions for some cases: e.g. in "myresources.v1alpha1.example.com".
# Non-conventional versions are indistinguishable from API groups ("myresources.foo1.example.com").
# See also: https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definition-versioning/
K8S_VERSION_PATTERN = re.compile(r'^v\d+(?:(?:alpha|beta)\d+)?$')


@dataclasses.dataclass(frozen=True, eq=False, repr=False)
class Resource:
    """
    A reference to a very specific custom or built-in resource kind.

    It is used to form the K8s API URLs. Generally, K8s API only needs
    an API group, an API version, and a plural name of the resource.
    All other names are remembered to match against resource selectors,
    for logging, and for informational purposes.
    """

    group: str
    """
    The resource's API group; e.g. ``"kopf.dev"``, ``"apps"``, ``"batch"``.
    For Core v1 API resources, an empty string: ``""``.
    """

    version: str
    """
    The resource's API version; e.g. ``"v1"``, ``"v1beta1"``, etc.
    """

    plural: str
    """
    The resource's plural name; e.g. ``"pods"``, ``"kopfexamples"``.
    It is used as an API endpoint, together with API group & version.
    """

    kind: Optional[str] = None
    """
    The resource's kind (as in YAML files); e.g. ``"Pod"``, ``"KopfExample"``.
    """

    singular: Optional[str] = None
    """
    The resource's singular name; e.g. ``"pod"``, ``"kopfexample"``.
    """

    shortcuts: FrozenSet[str] = frozenset()
    """
    The resource's short names; e.g. ``{"po"}``, ``{"kex", "kexes"}``.
    """

    categories: FrozenSet[str] = frozenset()
    """
    The resource's categories, to which the resource belongs; e.g. ``{"all"}``.
    """

    subresources: FrozenSet[str] = frozenset()
    """
    The resource's subresources, if defined; e.g. ``{"status", "scale"}``.
    """

    namespaced: Optional[bool] = None
    """
    Whether the resource is namespaced (``True``) or cluster-scoped (``False``).
    """

    preferred: bool = True  # against conventions, but makes versionless selectors match by default.
    """
    Whether the resource belong to a "preferred" API version.
    Only "preferred" resources are served when the version is not specified.
    """

    verbs: FrozenSet[str] = frozenset()
    """
    All available verbs for the resource, as supported by K8s API;
    e.g., ``{"list", "watch", "create", "update", "delete", "patch"}``.
    Note that it is not the same as all verbs permitted by RBAC.
    """

    def __hash__(self) -> int:
        return hash((self.group, self.version, self.plural))

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Resource):
            self_tuple = (self.group, self.version, self.plural)
            other_tuple = (other.group, other.version, other.plural)
            return self_tuple == other_tuple
        else:
            return NotImplemented

    def __repr__(self) -> str:
        plural_main, *subs = self.plural.split('/')
        name_text = f'{plural_main}.{self.version}.{self.group}'.strip('.')
        subs_text = f'/{"/".join(subs)}' if subs else ''
        return f'{name_text}{subs_text}'

    # Mostly for tests, to be used as `@kopf.on.event(*resource, ...)`
    def __iter__(self) -> Iterator[str]:
        return iter((self.group, self.version, self.plural))

    def get_url(
            self,
            *,
            server: Optional[str] = None,
            namespace: Namespace = None,
            name: Optional[str] = None,
            subresource: Optional[str] = None,
            params: Optional[Mapping[str, str]] = None,
    ) -> str:
        """
        Build a URL to be used with K8s API.

        If the namespace is not set, a cluster-wide URL is returned.
        For cluster-scoped resources, the namespace is ignored.

        If the name is not set, the URL for the resource list is returned.
        Otherwise (if set), the URL for the individual resource is returned.

        If subresource is set, that subresource's URL is returned,
        regardless of whether such a subresource is known or not.

        Params go to the query parameters (``?param1=value1&param2=value2...``).
        """
        if subresource is not None and name is None:
            raise ValueError("Subresources can be used only with specific resources by their name.")
        if not self.namespaced and namespace is not None:
            raise ValueError(f"Specific namespaces are not supported for cluster-scoped resources.")
        if self.namespaced and namespace is None and name is not None:
            raise ValueError("Specific namespaces are required for specific namespaced resources.")

        parts: List[Optional[str]] = [
            '/api' if self.group == '' and self.version == 'v1' else '/apis',
            self.group,
            self.version,
            'namespaces' if self.namespaced and namespace is not None else None,
            namespace if self.namespaced and namespace is not None else None,
            self.plural,
            name,
            subresource,
        ]

        query = urllib.parse.urlencode(params, encoding='utf-8') if params else ''
        path = '/'.join([part for part in parts if part])
        url = path + ('?' if query else '') + query
        return url if server is None else server.rstrip('/') + '/' + url.lstrip('/')


class Marker(enum.Enum):
    """
    A special marker to handle all resources possible, built-in and custom.
    """
    EVERYTHING = enum.auto()


# An explicit catch-all marker for positional arguments of resource selectors.
EVERYTHING = Marker.EVERYTHING


@dataclasses.dataclass(frozen=True)
class Selector:
    """
    A resource specification that can match several resource kinds.

    The resource specifications are not usable in K8s API calls, as the API
    has no endpoints with masks or placeholders for unknown or catch-all
    resource identifying parts (e.g. any API group, any API version, any name).

    They are used only locally in the operator to match against the actual
    resources with specific names (:class:`Resource`). The handlers are
    defined with resource specifications, but are invoked with specific
    resource kinds. Even if those specifications look very concrete and allow
    no variations, they still remain specifications.
    """

    arg1: dataclasses.InitVar[Union[None, str, Marker]] = None
    arg2: dataclasses.InitVar[Union[None, str, Marker]] = None
    arg3: dataclasses.InitVar[Union[None, str, Marker]] = None
    argN: dataclasses.InitVar[None] = None  # a runtime guard against too many positional arguments

    group: Optional[str] = None
    version: Optional[str] = None

    kind: Optional[str] = None
    plural: Optional[str] = None
    singular: Optional[str] = None
    shortcut: Optional[str] = None
    category: Optional[str] = None
    any_name: Optional[Union[str, Marker]] = None

    def __post_init__(
            self,
            arg1: Union[None, str, Marker],
            arg2: Union[None, str, Marker],
            arg3: Union[None, str, Marker],
            argN: None,  # a runtime guard against too many positional arguments
    ) -> None:

        # Since the class is frozen & read-only, post-creation field adjustment is done via a hack.
        # This is the same hack as used in the frozen dataclasses to initialise their fields.
        if argN is not None:
            raise TypeError("Too many positional arguments. Max 3 positional args are accepted.")
        elif arg3 is not None:
            object.__setattr__(self, 'group', arg1)
            object.__setattr__(self, 'version', arg2)
            object.__setattr__(self, 'any_name', arg3)
        elif arg2 is not None and isinstance(arg1, str) and '/' in arg1:
            object.__setattr__(self, 'group', arg1.rsplit('/', 1)[0])
            object.__setattr__(self, 'version', arg1.rsplit('/')[-1])
            object.__setattr__(self, 'any_name', arg2)
        elif arg2 is not None and arg1 == 'v1':
            object.__setattr__(self, 'group', '')
            object.__setattr__(self, 'version', arg1)
            object.__setattr__(self, 'any_name', arg2)
        elif arg2 is not None:
            object.__setattr__(self, 'group', arg1)
            object.__setattr__(self, 'any_name', arg2)
        elif arg1 is not None and isinstance(arg1, Marker):
            object.__setattr__(self, 'any_name', arg1)
        elif arg1 is not None and '.' in arg1 and K8S_VERSION_PATTERN.match(arg1.split('.')[1]):
            if len(arg1.split('.')) >= 3:
                object.__setattr__(self, 'group', arg1.split('.', 2)[2])
            object.__setattr__(self, 'version', arg1.split('.')[1])
            object.__setattr__(self, 'any_name', arg1.split('.')[0])
        elif arg1 is not None and '.' in arg1:
            object.__setattr__(self, 'group', arg1.split('.', 1)[1])
            object.__setattr__(self, 'any_name', arg1.split('.')[0])
        elif arg1 is not None:
            object.__setattr__(self, 'any_name', arg1)

        # Verify that explicit & interpreted arguments have produced an unambiguous specification.
        names = [self.kind, self.plural, self.singular, self.shortcut, self.category, self.any_name]
        clean = [name for name in names if name is not None]
        if len(clean) > 1:
            raise TypeError(f"Ambiguous resource specification with names {clean}")
        if len(clean) < 1:
            raise TypeError(f"Unspecific resource with no names.")

        # For reasons unknown, the singular is empty for ALL builtin resources. This does not affect
        # the checks unless defined as e.g. ``singular=""``, which would match ALL builtins at once.
        # Thus we prohibit it until clarified why is it so, what does it mean, how to deal with it.
        if any([name == '' for name in names]):
            raise TypeError("Names must not be empty strings; either None or specific strings.")

    def __repr__(self) -> str:
        kwargs = {f.name: getattr(self, f.name) for f in dataclasses.fields(self)}
        kwtext = ', '.join([f'{key!s}={val!r}' for key, val in kwargs.items() if val is not None])
        clsname = self.__class__.__name__
        return f'{clsname}({kwtext})'

    @property
    def is_specific(self) -> bool:
        return (self.kind is not None or
                self.shortcut is not None or
                self.plural is not None or
                self.singular is not None or
                (self.any_name is not None and not isinstance(self.any_name, Marker)))

    def check(self, resource: Resource) -> bool:
        """
        Check if a specific resources matches this resource specification.
        """
        # Core v1 events are excluded from EVERYTHING: they are implicitly produced during handling,
        # and thus trigger unnecessary handling cycles (even for other resources, not for events).
        return (
            (self.group is None or self.group == resource.group) and
            ((self.version is None and resource.preferred) or self.version == resource.version) and
            (self.kind is None or self.kind == resource.kind) and
            (self.plural is None or self.plural == resource.plural) and
            (self.singular is None or self.singular == resource.singular) and
            (self.category is None or self.category in resource.categories) and
            (self.shortcut is None or self.shortcut in resource.shortcuts) and
            (self.any_name is None or
             self.any_name == resource.kind or
             self.any_name == resource.plural or
             self.any_name == resource.singular or
             self.any_name in resource.shortcuts or
             (self.any_name is Marker.EVERYTHING and
              not EVENTS.check(resource) and
              not EVENTS_K8S.check(resource))))

    def select(self, resources: Collection[Resource]) -> Collection[Resource]:
        result = {resource for resource in resources if self.check(resource)}

        # Core v1 API group's priority is hard-coded in K8s and kubectl. Do the same. For example:
        # whenever "pods" is specified, and "pods.v1" & "pods.v1beta1.metrics.k8s.io" are found,
        # implicitly give priority to "v1" and hide the existence of non-"v1" groups.
        # But not if they are specified by categories! -- In that case, keep all resources as is.
        if self.is_specific:
            v1only = {resource for resource in result if resource.group == ''}
            result = v1only or result

        return result


# Some predefined API endpoints that we use in the framework itself (not exposed to the operators).
# Note: the CRDs are versionless: we do not look into its ``spec`` stanza, we only watch for
# the fact of changes, so the schema does not matter, any cluster-preferred API version would work.
# Note: the peering resources are either zalando.org/v1 or kopf.dev/v1; both cannot co-exist because
# they would share the names, so K8s will not let this. It is done for domain name transitioning.
CRDS = Selector('apiextensions.k8s.io', 'customresourcedefinitions')
EVENTS = Selector('v1', 'events')
EVENTS_K8S = Selector('events.k8s.io', 'events')  # only for exclusion from EVERYTHING
NAMESPACES = Selector('v1', 'namespaces')
CLUSTER_PEERINGS = Selector('clusterkopfpeerings')
NAMESPACED_PEERINGS = Selector('kopfpeerings')
MUTATING_WEBHOOK = Selector('admissionregistration.k8s.io', 'mutatingwebhookconfigurations')
VALIDATING_WEBHOOK = Selector('admissionregistration.k8s.io', 'validatingwebhookconfigurations')


class Backbone(Mapping[Selector, Resource]):
    """
    Actual resources used in the core (reactor & engines) of the framework.

    Why? The codebase only refers to the resources by API group/version & names.
    The actual resources can be different in different clusters, usually due
    to different versions: e.g. "v1" vs. "v1beta1" for CRDs.

    The actual backbone resources are detected in the initial cluster scanning
    during the operator startup in :func:`resource_scanner`.

    The backbone resources cannot be changed at runtime after they are found
    for the first time -- since the core tasks are already started with those
    resource definitions, and cannot be easily restarted.

    This does not apply to the resources of the operator (not the framework!),
    where the resources can be created, changed, and deleted at runtime easily.
    """

    def __init__(self) -> None:
        super().__init__()
        self._items: MutableMapping[Selector, Resource] = {}
        self._revised = asyncio.Condition()
        self.selectors = [
            NAMESPACES, EVENTS, CRDS,
            CLUSTER_PEERINGS, NAMESPACED_PEERINGS,
            MUTATING_WEBHOOK, VALIDATING_WEBHOOK,
        ]

    def __len__(self) -> int:
        return len(self._items)

    def __iter__(self) -> Iterator[Selector]:
        return iter(self._items)

    def __getitem__(self, item: Selector) -> Resource:
        return self._items[item]

    async def fill(
            self,
            *,
            resources: Iterable[Resource],
    ) -> None:
        async with self._revised:
            for resource in resources:
                for spec in self.selectors:
                    if spec not in self._items:
                        if spec.check(resource):
                            self._items[spec] = resource
            self._revised.notify_all()

    async def wait_for(
            self,
            selector: Selector,
    ) -> Resource:
        """
        Wait for the actual resource to be found in the cluster scanning.

        The resources can be cached in-memory. Once the resource is retrieved,
        it never changes in memory even if it changes in the cluster. This is
        intentional -- to match with the nature of the cluster scanning,
        which waits for the resources and then starts background jobs,
        which are not easy to terminate without terminating the whole operator.
        """
        async with self._revised:
            await self._revised.wait_for(lambda: selector in self)
        return self[selector]


@dataclasses.dataclass(frozen=True)
class Insights:
    """
    Actual resources & namespaces served by the operator.
    """
    namespaces: Set[Namespace] = dataclasses.field(default_factory=set)
    resources: Set[Resource] = dataclasses.field(default_factory=set)
    backbone: Backbone = dataclasses.field(default_factory=Backbone)

    # Signalled when anything changes in the insights.
    revised: asyncio.Condition = dataclasses.field(default_factory=asyncio.Condition)

    # The flags that are set after the initial listing is finished. Not cleared afterwards.
    ready_namespaces: asyncio.Event = dataclasses.field(default_factory=asyncio.Event)
    ready_resources: asyncio.Event = dataclasses.field(default_factory=asyncio.Event)

    # The resources that are part of indices and can block the operator readiness on start.
    indexable: Set[Resource] = dataclasses.field(default_factory=set)
