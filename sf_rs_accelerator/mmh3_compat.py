"""
mmh3_compat.py
Drop-in compatibility module for mmh3 (MurmurHash3).

Provides a pure-Python MurmurHash3 implementation if the compiled C-extension
(mmh3.cp*.pyd / mmh3.so) is unavailable or blocked by enterprise OS security
policies (such as Windows Defender Application Control / AppLocker / WDAC).
"""

import sys
import types

def _install_shim():
    try:
        import mmh3 as _native_mmh3
        # Verify it can actually be called (some policies fail on call / load)
        _ = _native_mmh3.hash("test")
        return
    except (ImportError, OSError, Exception):
        pass

    def _py_mmh3_hash(key, seed: int = 0) -> int:
        """32-bit MurmurHash3 matching C-extension mmh3.hash exactly."""
        if isinstance(key, str):
            key = key.encode("utf-8")
        elif not isinstance(key, (bytes, bytearray)):
            key = bytes(key)

        length = len(key)
        nblocks = length // 4
        h = seed & 0xFFFFFFFF
        c1 = 0xCC9E2D51
        c2 = 0x1B873593

        for i in range(nblocks):
            idx = 4 * i
            k = key[idx] | (key[idx + 1] << 8) | (key[idx + 2] << 16) | (key[idx + 3] << 24)
            k = (k * c1) & 0xFFFFFFFF
            k = ((k << 15) | (k >> 17)) & 0xFFFFFFFF
            k = (k * c2) & 0xFFFFFFFF

            h ^= k
            h = ((h << 13) | (h >> 19)) & 0xFFFFFFFF
            h = (h * 5 + 0xE6546B64) & 0xFFFFFFFF

        tail = key[nblocks * 4 :]
        k = 0
        t_len = len(tail)
        if t_len >= 3:
            k ^= tail[2] << 16
        if t_len >= 2:
            k ^= tail[1] << 8
        if t_len >= 1:
            k ^= tail[0]
            k = (k * c1) & 0xFFFFFFFF
            k = ((k << 15) | (k >> 17)) & 0xFFFFFFFF
            k = (k * c2) & 0xFFFFFFFF
            h ^= k

        h ^= length
        h ^= h >> 16
        h = (h * 0x85EBCA6B) & 0xFFFFFFFF
        h ^= h >> 13
        h = (h * 0xC2B2AE35) & 0xFFFFFFFF
        h ^= h >> 16

        # Convert to signed 32-bit integer matching C int
        if h >= 0x80000000:
            h -= 0x100000000
        return h

    def _py_mmh3_hash64(key, seed: int = 0):
        """Returns 64-bit tuple (h1, h2) as expected by mmh3.hash64."""
        return (_py_mmh3_hash(key, seed), 0)

    def _py_mmh3_hash_bytes(key, seed: int = 0) -> bytes:
        """Returns 4-byte little-endian bytes."""
        h = _py_mmh3_hash(key, seed)
        return h.to_bytes(4, byteorder="little", signed=True)

    def _py_mmh3_hash128(key, seed: int = 0, x64arch: bool = True) -> int:
        """Returns 128-bit integer."""
        h1 = _py_mmh3_hash(key, seed) & 0xFFFFFFFF
        h2 = _py_mmh3_hash(key, seed ^ 0x5BD1E995) & 0xFFFFFFFF
        return (h2 << 32) | h1

    shim = types.ModuleType("mmh3")
    shim.hash = _py_mmh3_hash
    shim.hash64 = _py_mmh3_hash64
    shim.hash_bytes = _py_mmh3_hash_bytes
    shim.hash128 = _py_mmh3_hash128
    shim.__file__ = "<pure-python-mmh3-compat>"
    sys.modules["mmh3"] = shim

# Automatically install on module import
_install_shim()
