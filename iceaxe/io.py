import asyncio
import importlib.metadata
from functools import lru_cache, wraps
from json import loads as json_loads
from pathlib import Path
from re import search as re_search
from typing import Any, Callable, Coroutine, TypeVar

T = TypeVar("T")


def lru_cache_async(
    maxsize: int | None = 100,
):
    def decorator(
        async_function: Callable[..., Coroutine[Any, Any, T]],
    ):
        @lru_cache(maxsize=maxsize)
        @wraps(async_function)
        def internal(*args, **kwargs):
            coroutine = async_function(*args, **kwargs)
            # Unlike regular coroutine functions, futures can be awaited multiple times
            # so our caller functions can await the same future on multiple cache hits
            return asyncio.ensure_future(coroutine)

        return internal

    return decorator


def resolve_package_path(package_name: str):
    """
    Given a package distribution, returns the local file directory where the code
    is located. This is resolved to the original reference if installed with `-e`
    otherwise is a copy of the package.

    NOTE: Copied from Mountaineer, refactor to a shared library.

    """
    dist = importlib.metadata.distribution(package_name)

    def normalize_package(package: str):
        return package.replace("-", "_").lower()

    # Recent versions of poetry install development packages (-e .) as direct URLs
    # https://the-hitchhikers-guide-to-packaging.readthedocs.io/en/latest/introduction.html
    # "Path configuration files have an extension of .pth, and each line must
    # contain a single path that will be appended to sys.path."
    package_name = normalize_package(dist.name)
    symbolic_links = [
        path
        for path in (dist.files or [])
        if path.name.lower() == f"{package_name}.pth"
    ]
    dist_links = [
        path
        for path in (dist.files or [])
        if path.name == "direct_url.json"
        and re_search(package_name + r"-[0-9-.]+\.dist-info", path.parent.name.lower())
    ]
    explicit_links = [
        path
        for path in (dist.files or [])
        if path.parent.name.lower() == package_name
        and (
            # Sanity check that the parent is the high level project directory
            # by looking for common base files
            path.name == "__init__.py"
        )
    ]

    # The user installed code as an absolute package (ie. with pip install .) instead of
    # as a reference. There's no need to sniff for the additional package path since
    # we've already found it
    if explicit_links:
        # Right now we have a file pointer to __init__.py. Go up one level
        # to the main package directory to return a directory
        explicit_link = explicit_links[0]
        return Path(str(dist.locate_file(explicit_link.parent)))

    # Raw path will capture the path to the pyproject.toml file directory,
    # not the actual package code directory
    # Find the root, then resolve the package directory
    raw_path: Path | None = None

    if symbolic_links:
        direct_url_path = symbolic_links[0]
        raw_path = Path(str(dist.locate_file(direct_url_path.read_text().strip())))
    elif dist_links:
        dist_link = dist_links[0]
        direct_metadata = json_loads(dist_link.read_text())
        package_path = "/" + direct_metadata["url"].lstrip("file://").lstrip("/")
        raw_path = Path(str(dist.locate_file(package_path)))

    if not raw_path:
        raise ValueError(
            f"Could not find a valid path for package {dist.name}, found files: {dist.files}"
        )

    # Sniff for the presence of the code directory
    for path in raw_path.iterdir():
        if path.is_dir() and normalize_package(path.name) == package_name:
            return path

    raise ValueError(
        f"No matching package found in root path: {raw_path} {list(raw_path.iterdir())}"
    )
