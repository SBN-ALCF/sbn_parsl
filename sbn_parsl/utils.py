import pathlib
import hashlib


def hash_name(string: str, maxlen: int = 16, sep: str = "-") -> str:
    """Create something that looks like abcd-abcd-abcd-abcd from a string."""
    strhash = hashlib.sha256(string.encode("utf-8")).hexdigest()[: max(maxlen, 2)]
    return sep.join(strhash[i * 4 : i * 4 + 4] for i in range(4))


def subrun_dir(
    prefix: pathlib.Path, subrun: int, step: int = 2, depth: int = 2, width: int = 6
):
    """
    Returns a path with directory structure like XXXX00/XXXXXX.
    Number of 0s set by depth. Left padding set by width.
    """
    width = max(width, len(str(subrun)))
    result = prefix
    if depth < 1:
        raise RuntimeError("Must set depth >= 1")

    for i in reversed(range(0, depth)):
        q = 10 ** (step * i)
        path_element = q * (subrun // q)
        result /= f"{path_element:0{width}d}"

    return result


def get_subrun_dir(prefix: pathlib.Path, subrun: int) -> pathlib.Path:
    """
    Consolidated helper for standard subrun directory structure:
    prefix / XXXXX0 / XXXXXX / subrun_XXXXXX
    """
    return (
        prefix
        / f"{(subrun // 1000):06d}"
        / f"{(subrun // 100):06d}"
        / f"subrun_{subrun:06d}"
    )


def get_caf_dir(prefix: pathlib.Path, subrun: int) -> pathlib.Path:
    """
    Consolidated helper for standard CAF directory structure:
    prefix / XXXXX0 / caf / subrun_XXXXXX
    """
    return prefix / f"{(subrun // 1000):06d}" / "caf" / f"subrun_{subrun:06d}"
