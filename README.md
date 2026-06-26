# sbn_parsl

Classes and functions for running LArSoft workflows with Parsl on HPC clusters (e.g., Polaris and Aurora at ALCF).

## Setup Instructions

The project uses `uv` for dependency management and virtual environments.

### 1. Install uv
If you don't have `uv` installed, follow the [official installation guide](https://github.com/astral-sh/uv).

### 2. Initialize the environment
```bash
# Clone the repository
git clone <repo-url>
cd sbn_parsl

# Create a virtual environment and install dependencies
uv venv
source .venv/bin/activate  # On macOS/Linux
# OR
uv run ... # Use uv run to execute commands in the environment
```

### 3. Install in editable mode
```bash
uv pip install -e .
```

### Troubleshooting: "no module named sqlite3"

If you receive a `ModuleNotFoundError: No module named 'sqlite3'` error during execution, this means your active Python interpreter lacks SQLite support.

*   **Using `uv`-managed Python (Recommended)**: Force `uv` to install a clean standalone Python package and recreate the virtual environment:
    ```bash
    uv python install 3.12 --reinstall
    uv python pin 3.12
    rm -rf .venv
    uv venv
    uv sync
    ```
*   **Using System Python**: Ensure SQLite development libraries are installed on your host system:
    *   *Debian/Ubuntu*: `sudo apt install libsqlite3-dev`
    *   *RedHat/Rocky Linux*: `sudo dnf install sqlite-devel`
    And then reinstall Python so it compiles against the SQLite dev headers.


## Configuration System

The configuration system is split into two parts: **Site Settings** and **Workflow Settings**.

### Site Settings
Located in `settings/sites/`, these files define machine-specific and environment-specific parameters.
- **`[site]`**: Hardware parameters like `cpus_per_node`, `scheduler_options`, and the optional `virtual_env` path (which defaults to auto-detecting your active virtual or conda environment on the submit host).
- **`[larsoft]`**: Software environment defaults like `container_path`, `larsoft_top`, and `metadata_exe`.

### Workflow Settings
These are the TOML files you pass to the workflow scripts (e.g., in `settings/sbnd/` or `settings/icarus/`).
- **`[larsoft]`**: Experiment-specific software versions, qualifiers, and experiment names.
- **`[workflow]`**: Logic-specific parameters like `subruns_per_caf` or `fcls`.
- **`[job]`**: Submission-specific overrides like `allocation`, `queue`, and `walltime`.
- **`[run]`**: High-level execution properties like `nsubruns`. Note: `output` is now mandatory via the CLI and should not be in these files.
- **`[metadata]`**: Metadata injection parameters like `mdprojectversion`.

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

## Usage

`sbn_parsl` provides a structured workflow execution framework using three primary levels of organization:

- **`Stage`**: Represents a single step or task within the workflow (e.g., generating events, running Geant4, executing a specific FCL file).
- **`Workflow`**: Composes multiple `Stage` objects together, defining the execution order and dependencies.
- **`LArSoftExecutor`**: Uses `parsl` to configure workflows based on a strongly typed TOML settings schema and automatically maps outputs to inputs based on the FCL files. 

### Basic Example

#### 1. Configuration (`settings.toml`)

```toml
[larsoft]
experiment = "sbnd"
version = "v1"
qual = "e26:prof"

[workflow.fcls]
gen = "gen.fcl"
g4 = "g4.fcl"
detsim = "detsim.fcl"
```

#### 2. Workflow Script (`my_workflow.py`)

```python
#!/usr/bin/env python3
import sys
from sbn_parsl.workflow import StageType, Stage, Workflow, LArSoftExecutor, StageResult
from sbn_parsl.app import entry_point, parse_arguments

class SimpleWorkflowExecutor(LArSoftExecutor):
    """A minimal executor to run GEN -> G4 -> DETSIM"""
    
    def __init__(self, cfg):
        super().__init__(cfg)
        self.stage_order = [StageType.from_str(k) for k in self.fcls.keys()]

    def setup_single_workflow(self, iteration: int, inputs=None, last_file=None):
        workflow = Workflow(self.stage_order, default_fcls=self.fcls)
        s = Stage(StageType.from_str("detsim"))
        s.run_dir = self.get_run_dir(iteration)
        workflow.add_final_stage(s)
        return workflow

if __name__ == '__main__':
    entry_point(sys.argv, SimpleWorkflowExecutor)
```

Running the above script:
```bash
python my_workflow.py settings.toml -o ./output/
```

## For Developers

### Project Structure
- **`sbn_parsl/`**: Core library code.
    - `config.py`: Configuration schema definitions using Python dataclasses.
    - `workflow.py`: Core logic for `Stage`, `Workflow`, and `LArSoftExecutor`.
    - `app.py`: CLI entry point and argument parsing.
    - `components.py`: Parsl app definitions and LArSoft execution logic.
    - `experiments/`: Experiment-specific run functions (e.g., SBND, ICARUS).
- **`settings/`**: TOML configuration templates.
    - `sites/`: Site-specific configurations (Polaris, Aurora, Local).
    - `sbnd/` & `icarus/`: Experiment-specific workflow settings.
- **`workflows/`**: Implementation of specific workflows (e.g., `sbnd_mc.py`).
- **`tests/`**: Pytest suite covering CLI, config, and workflow logic.
- **`tools/`**: Utility scripts for monitoring and submission.

### Testing
Run the test suite using `uv`:
```bash
uv run pytest
```
