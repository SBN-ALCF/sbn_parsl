# sbn_parsl

Classes and functions for running LArSoft workflows with Parsl on HPC clusters (e.g., Polaris and Aurora at ALCF).

## Installation

To install an editable version in a conda environment, use
```bash
conda develop .
```

If using a Python virtual environment with `pip`, use

```bash
pip install -e .
```

## Usage

`sbn_parsl` provides a structured workflow execution framework using three primary levels of organization:

- **`Stage`**: Represents a single step or task within the workflow (e.g., generating events, running Geant4, executing a specific FCL file).
- **`Workflow`**: Composes multiple `Stage` objects together, defining the execution order and dependencies.
- **`LArSoftExecutor`**: Uses `parsl` to configure workflows based on a strongly typed TOML settings schema and automatically maps outputs to inputs based on the FCL files. 

### Basic Example

`sbn_parsl` now strictly uses TOML configuration files mapped to Python `dataclasses`.

#### 1. Configuration (`settings.toml`)

```toml
[run]
nsubruns = 2

[larsoft]
experiment = "sbnd"
version = "v1"
qual = "e26:prof"

[fcls]
gen = "gen.fcl"
g4 = "g4.fcl"
detsim = "detsim.fcl"
```

#### 2. Workflow Script (`my_workflow.py`)

Below is a Minimum Working Example (MWE) showing how to set up a `LArSoftExecutor` using the new typed configuration:

```python
#!/usr/bin/env python3
import sys
from sbn_parsl.workflow import StageType, Stage, Workflow, LArSoftExecutor, StageResult
from sbn_parsl.app import entry_point, parse_arguments

class SimpleWorkflowExecutor(LArSoftExecutor):
    """A minimal executor to run GEN -> G4 -> DETSIM"""
    
    def __init__(self, cfg):
        super().__init__(cfg)
        # We can dynamically define the order of stages based on the FCL keys in TOML
        self.stage_order = [StageType.from_str(k) for k in self.fcls.keys()]

    def setup_single_workflow(self, iteration: int, inputs=None, last_file=None):
        workflow = Workflow(self.stage_order, default_fcls=self.fcls)
        
        # We just need to define the final stage in our desired order.
        # sbn_parsl will automatically build the parent dependency tree backwards.
        s = Stage(StageType.from_str("detsim"))
        s.run_dir = self.output_dir / f"subrun_{iteration:04d}"
        
        workflow.add_final_stage(s)
        return workflow

if __name__ == '__main__':
    # Parse CLI overrides (like -o for output, --local for local execution)
    args = parse_arguments(sys.argv)
    
    # Hand off execution to sbn_parsl to build Parsl configs and run workflows
    entry_point(args, SimpleWorkflowExecutor)
```

Running the above script:
```bash
python my_workflow.py settings.toml -o ./output/
```

### Implementing Custom Runfuncs

To customize how a stage behaves during execution, you can define your own `runfunc`. A `runfunc` takes the current `Stage` instance, the FCL file path, the `StageResult` containing inputs/dependencies from its parent, and the output directory.

**A `runfunc` must return a `StageResult` object containing its outputs.**

```python
import functools
from sbn_parsl.workflow import StageResult

def my_runfunc(self, fcl, parent_result: StageResult, output_dir, executor: LArSoftExecutor) -> StageResult:
    """A custom function to execute a LArSoft bash command via parsl."""
    
    input_files = parent_result.outputs
    # E.g. build a custom command using executor.cfg ...
    
    output_filepath = output_dir / f"custom_{fcl.replace('.fcl', '.root')}"
    
    # Normally you'd submit a Parsl @bash_app here and return its Future.
    # future = fcl_future(outputs=[File(str(output_filepath))])
    # return StageResult(outputs=future.outputs)

    return StageResult(outputs=[output_filepath])

class SimpleWorkflowExecutor(LArSoftExecutor):
    ...
    def setup_single_workflow(self, iteration: int, inputs=None, last_file=None):
        ...
        s = Stage(StageType.from_str("detsim"))
        
        # Bind the custom runfunc to this stage
        s.runfunc = functools.partial(my_runfunc, executor=self) 
        ...
```

### Overriding Configuration from CLI
The configuration system natively supports bypassing properties from the command line:

```bash
# Run over 50 subruns, overriding the TOML default
python my_workflow.py settings.toml -o ./out/ --nsubruns 50 

# Ignore site defaults and use specific queue settings
python my_workflow.py settings.toml -o ./out/ -A my_allocation -q debug -t 01:00:00

# Execute entirely locally (e.g. on a login node or local server) without PBS 
python my_workflow.py settings.toml -o ./out/ --local
```
