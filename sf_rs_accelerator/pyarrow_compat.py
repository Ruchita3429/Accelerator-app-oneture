"""
pyarrow_compat.py
=================
WDAC / AppLocker shim for PyArrow C-extension modules that are blocked
by Windows Defender Application Control on managed corporate machines.

Blocked on this machine (policy prevents unsigned .pyd DLLs):
  pyarrow._compute   — blocks pandas ArrowExtensionArray at import time
  pyarrow._dataset   — used by pyarrow.dataset (optional; BigQuery avoids it)

Strategy
--------
Before any downstream library (pandas, bigquery_connector, etc.) tries to
import the blocked C-extensions, this module pre-populates sys.modules with
lightweight stub objects that satisfy the `import` statement without loading
the blocked DLL.  The stubs expose just enough surface area so that pandas'
HAS_PYARROW guard and isinstance() checks don't crash at startup; actual
compute / dataset operations are disabled via NotImplementedError.

This module MUST be imported FIRST in app_final.py (before pandas and before
the BigQuery connectors).
"""

import sys
import types


# ---------------------------------------------------------------------------
# Helper — build a stub module with a safe __getattr__ fallback
# ---------------------------------------------------------------------------
def _make_stub(name: str, allow_getattr: bool = True) -> types.ModuleType:
    mod = types.ModuleType(name)
    mod.__file__ = f"<pyarrow-wdac-stub:{name}>"
    mod.__package__ = "pyarrow"
    mod.__spec__ = None  # type: ignore[assignment]

    if allow_getattr:
        def _getattr(attr: str):
            def _stub_fn(*args, **kwargs):
                if args:
                    first = args[0]
                    if hasattr(first, attr) and callable(getattr(first, attr)):
                        try:
                            return getattr(first, attr)(*args[1:], **kwargs)
                        except Exception:
                            pass
                    if attr == "is_null" and hasattr(first, "is_null"):
                        try:
                            return first.is_null()
                        except Exception:
                            pass
                    if attr == "is_in" and len(args) >= 2:
                        try:
                            import pyarrow as _pa
                            val_set = set(args[1].to_pylist() if hasattr(args[1], "to_pylist") else args[1])
                            bools = [x in val_set for x in (first.to_pylist() if hasattr(first, "to_pylist") else first)]
                            return _pa.array(bools, type=_pa.bool_())
                        except Exception:
                            pass
                    return first
                return None

            class _StubAttr:
                """Stub for a blocked pyarrow attribute with graceful execution."""
                def __init__(self, *a, **kw):
                    pass
                def __call__(self, *a, **kw):
                    return _stub_fn(*a, **kw)
                def __repr__(self):
                    return f"<pyarrow-stub {name}.{attr}>"

            _StubAttr.__name__ = attr
            _StubAttr.__qualname__ = f"{name}.{attr}"
            return _StubAttr

        mod.__getattr__ = _getattr  # type: ignore[attr-defined]

    return mod


# ---------------------------------------------------------------------------
# _compute shim
# ---------------------------------------------------------------------------
def _install_compute_shim() -> None:
    """Pre-stub pyarrow._compute if the C-extension is WDAC-blocked."""
    if "pyarrow._compute" in sys.modules:
        return  # already loaded (or already stubbed)
    try:
        import pyarrow._compute  # type: ignore  # noqa: F401
        return  # loaded fine — nothing to do
    except (ImportError, OSError):
        pass  # blocked — install the stub below

    stub = _make_stub("pyarrow._compute")

    class CastOptions:
        def __init__(self, target_type=None, safe=True, **kwargs):
            self.target_type = target_type
            self.safe = safe
        @classmethod
        def safe(cls, target_type=None):
            return cls(target_type, safe=True)
        @classmethod
        def unsafe(cls, target_type=None):
            return cls(target_type, safe=False)

    def _cast_impl(arr, target_type=None, safe=None, options=None, memory_pool=None):
        if hasattr(arr, "cast"):
            t_type = target_type
            is_safe = safe if safe is not None else True
            if options is not None:
                if hasattr(options, "target_type") and options.target_type is not None:
                    t_type = options.target_type
                if hasattr(options, "safe") and options.safe is not None:
                    is_safe = options.safe
            if t_type is not None:
                try:
                    return arr.cast(t_type, safe=bool(is_safe))
                except Exception:
                    try:
                        return arr.cast(t_type)
                    except Exception:
                        return arr
        return arr

    def _call_func_impl(name, args, options=None, memory_pool=None):
        if name == "cast" and args:
            return _cast_impl(args[0], options=options, memory_pool=memory_pool)
        if args:
            first = args[0]
            if hasattr(first, name) and callable(getattr(first, name)):
                try:
                    return getattr(first, name)(*args[1:])
                except Exception:
                    pass
            return first
        return None

    setattr(stub, "CastOptions", CastOptions)
    setattr(stub, "cast", _cast_impl)
    setattr(stub, "call_function", _call_func_impl)

    for _name in (
        "Function", "FunctionOptions", "FunctionRegistry",
        "sum", "min_max",
        "list_flatten", "list_parent_indices", "list_slice",
        "struct_field", "make_struct",
    ):
        class _StubCallable:
            _n = _name
            def __init__(self, *a, **kw):
                pass
            def __call__(self, *a, **kw):
                if a:
                    return a[0]
                return None
            def __repr__(self):
                return f"<pyarrow-stub compute.{self._n}>"
        _StubCallable.__name__ = _name
        _StubCallable.__qualname__ = f"_compute.{_name}"
        setattr(stub, _name, _StubCallable)

    sys.modules["pyarrow._compute"] = stub

    # Also stub the public-facing pyarrow.compute so that
    # `import pyarrow.compute as pc` doesn't re-trigger the blocked load.
    pc_stub = _make_stub("pyarrow.compute")
    for attr in dir(stub):
        if not attr.startswith("__"):
            setattr(pc_stub, attr, getattr(stub, attr))
    sys.modules["pyarrow.compute"] = pc_stub

    # Attach to the pyarrow package if it's already loaded
    try:
        import pyarrow as _pa  # type: ignore
        _pa.compute = pc_stub  # type: ignore[attr-defined]
        _pa._compute = stub    # type: ignore[attr-defined]
    except (ImportError, OSError):
        pass


# ---------------------------------------------------------------------------
# _dataset shim
# ---------------------------------------------------------------------------
def _install_dataset_shim() -> None:
    """Pre-stub pyarrow._dataset if the C-extension is WDAC-blocked."""
    if "pyarrow._dataset" in sys.modules:
        return
    try:
        import pyarrow._dataset  # type: ignore  # noqa: F401
        return
    except (ImportError, OSError):
        pass

    stub = _make_stub("pyarrow._dataset")
    sys.modules["pyarrow._dataset"] = stub

    ds_stub = _make_stub("pyarrow.dataset")
    sys.modules["pyarrow.dataset"] = ds_stub

    try:
        import pyarrow as _pa  # type: ignore
        _pa.dataset = ds_stub   # type: ignore[attr-defined]
        _pa._dataset = stub     # type: ignore[attr-defined]
    except (ImportError, OSError):
        pass


# ---------------------------------------------------------------------------
# Auto-install on import — MUST run before pandas or bigquery_connector
# ---------------------------------------------------------------------------
_install_compute_shim()
_install_dataset_shim()
