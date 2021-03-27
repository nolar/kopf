from typing import NewType

# Strings are taken from the users, but then tainted as this type for stricter type-checking:
# to prevent usage of some other strings (e.g. operator id) as the handlers ids.
# It is so much ubiquitous that it deserves its own module to avoid circular dependencies.
HandlerId = NewType('HandlerId', str)
