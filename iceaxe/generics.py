import types
from inspect import isclass
from typing import Any, Type, TypeGuard, TypeVar, Union, get_args, get_origin


def mro_distance(obj_type: Type, target_type: Type) -> float:
    """
    Calculate the MRO distance between obj_type and target_type.
    Returns a large number if no match is found.

    """
    if not isclass(obj_type):
        obj_type = type(obj_type)
    if not isclass(target_type):
        target_type = type(target_type)

    # Compare class types for exact match
    if obj_type == target_type:
        return 0

    # Check if obj_type is a subclass of target_type using the MRO
    try:
        return obj_type.mro().index(target_type)  # type: ignore
    except ValueError:
        return float("inf")


T = TypeVar("T")


def is_type_compatible(obj_type: Type, target_type: T) -> TypeGuard[T]:
    return _is_type_compatible(obj_type, target_type) < float("inf")


def _is_type_compatible(obj_type: Type, target_type: Any) -> float:
    """
    Relatively comprehensive type compatibility checker. This function is
    used to check if a type has has a registered object that can
    handle it.

    Specifically returns the MRO distance where 0 indicates
    an exact match, 1 indicates a direct ancestor, and so on. Returns a large number
    if no compatibility is found.

    """
    # Any type is compatible with any other type
    if target_type is Any:
        return 0

    # If obj_type is a nested type, each of these types must be compatible
    # with the corresponding type in target_type
    if get_origin(obj_type) is Union or isinstance(obj_type, types.UnionType):
        return max(_is_type_compatible(t, target_type) for t in get_args(obj_type))

    # Handle OR types
    if get_origin(target_type) is Union or isinstance(target_type, types.UnionType):
        return min(_is_type_compatible(obj_type, t) for t in get_args(target_type))

    # Handle Type[Values] like typehints where we want to typehint a class
    if get_origin(target_type) == type:  # noqa: E721
        return _is_type_compatible(obj_type, get_args(target_type)[0])

    # Handle dict[str, str] like typehints
    # We assume that each arg in order must be matched with the target type
    obj_origin = get_origin(obj_type)
    target_origin = get_origin(target_type)
    if obj_origin and target_origin:
        if obj_origin == target_origin:
            return max(
                _is_type_compatible(t1, t2)
                for t1, t2 in zip(get_args(obj_type), get_args(target_type))
            )
        else:
            return float("inf")

    # For lists, sets, and tuple objects make sure that each object matches
    # the target type
    if isinstance(obj_type, (list, set, tuple)):
        if type(obj_type) != get_origin(target_type):  # noqa: E721
            return float("inf")
        return max(
            _is_type_compatible(obj, get_args(target_type)[0]) for obj in obj_type
        )

    if isinstance(target_type, type):
        return mro_distance(obj_type, target_type)

    # Default case
    return float("inf")


def remove_null_type(typehint: Type) -> Type:
    if get_origin(typehint) is Union or isinstance(typehint, types.UnionType):
        return Union[  # type: ignore
            tuple(  # type: ignore
                [t for t in get_args(typehint) if t != type(None)]  # noqa: E721
            )
        ]
    return typehint


def has_null_type(typehint: Type) -> bool:
    if get_origin(typehint) is Union or isinstance(typehint, types.UnionType):
        return any(arg == type(None) for arg in get_args(typehint))  # noqa: E721
    return typehint == type(None)  # noqa: E721


def get_typevar_mapping(cls):
    """
    Get the raw typevar mappings {typvar: generic values} for each
    typevar in the class hierarchy of `cls`.

    Shared logic with Mountaineer. TODO: Move to a shared package.

    """
    mapping: dict[Any, Any] = {}

    # Traverse MRO in reverse order, except `object`
    for base in reversed(cls.__mro__[:-1]):
        # Skip non-generic classes
        if not hasattr(base, "__orig_bases__"):
            continue

        for origin_base in base.__orig_bases__:
            origin = get_origin(origin_base)
            if origin:
                base_params = getattr(origin, "__parameters__", [])
                instantiated_params = get_args(origin_base)

                # Update mapping with current base's mappings
                base_mapping = dict(zip(base_params, instantiated_params))
                for key, value in base_mapping.items():
                    # If value is another TypeVar, resolve it if possible
                    if isinstance(value, TypeVar) and value in mapping:
                        mapping[key] = mapping[value]
                    else:
                        mapping[key] = value

    # Exclude TypeVars from the final mapping
    return mapping
