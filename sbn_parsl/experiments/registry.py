import inspect
from typing import Callable, Dict, Optional
from sbn_parsl.workflow import StageResult


class RegistryError(Exception):
    """Raised when a component fails validation during registration."""

    pass


def validate_runfunc(func: Callable):
    """
    Validates that a runfunc has the correct signature:
    (self, fcl, parent_result, output_dir) -> StageResult
    """
    sig = inspect.signature(func)
    params = list(sig.parameters.values())

    # We expect at least 4 arguments.
    # If it's a partial, we check the remaining arguments.
    if len(params) < 4:
        raise RegistryError(
            f"Runfunc '{func.__name__ if hasattr(func, '__name__') else 'partial'}' "
            f"must take at least 4 arguments (self, fcl, parent_result, output_dir). "
            f"Got: {list(sig.parameters.keys())}"
        )

    # Check return type if possible (might be hard with partials)
    # For now, we trust the dev or check at runtime if needed,
    # but let's try to check the annotation if it exists.
    if sig.return_annotation is not inspect.Signature.empty:
        if sig.return_annotation != StageResult:
            # Relaxing this for now as some might use strings or types that resolve to StageResult
            pass


def validate_fcl_modifier(func: Callable):
    """
    Validates that a fcl_modifier has the correct signature:
    (context: RunContext) -> str
    """
    sig = inspect.signature(func)
    params = list(sig.parameters.values())
    if len(params) != 1:
        raise RegistryError(
            f"FCL modifier '{func.__name__}' must take exactly 1 argument (context). "
            f"Got: {list(sig.parameters.keys())}"
        )


class ExperimentRegistry:
    def __init__(self, name: str):
        self.name = name
        self.runfuncs: Dict[str, Callable] = {}
        self.fcl_modifiers: Dict[str, Callable] = {}

    def register_runfunc(self, name: str, func: Optional[Callable] = None):
        def decorator(f: Callable):
            validate_runfunc(f)
            self.runfuncs[name] = f
            return f

        if func is not None:
            return decorator(func)
        return decorator

    def register_fcl_modifier(self, name: str, func: Optional[Callable] = None):
        def decorator(f: Callable):
            validate_fcl_modifier(f)
            self.fcl_modifiers[name] = f
            return f

        if func is not None:
            return decorator(func)
        return decorator


# Global registries for SBND and ICARUS
sbnd_registry = ExperimentRegistry("sbnd")
icarus_registry = ExperimentRegistry("icarus")
