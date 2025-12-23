from __future__ import annotations


def col_index_to_a1(col_index: int) -> str:
    """0 -> A, 1 -> B, ..., 25 -> Z, 26 -> AA, ..."""
    if col_index < 0:
        raise ValueError("col_index must be >= 0")

    n = col_index + 1
    letters: list[str] = []
    while n > 0:
        n, rem = divmod(n - 1, 26)
        letters.append(chr(ord("A") + rem))
    return "".join(reversed(letters))
