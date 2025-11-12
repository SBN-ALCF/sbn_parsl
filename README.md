# sbnd_parsl

Classes and functions for running SBND workflows on Polaris at ALCF.

## Installation

To install an editable version in a conda environment, use
```
conda develop .
```

If using a Python virtual environment with `pip`, use

```
pip install -e .
```

## Usage

`sbnd_parsl` provides a three-level class structure for structuring job submission:

- `Stage` objects for handling individual tasks, e.g., optionally modifying & running a single FCL file
- `Workflow` objects for composing multiple stages together with dependencies on the outputs of other stages
- `WorkflowExecutor` objects for configuring workflows from user settings, and executing multiple copies of a Workflow for scaling to large jobs

### Basic Example

Below, we set up a `WorkflowExecutor` and implement its `setup_single_workflow` function:

```python
#!/usr/bin/env python3
import pathlib

from sbnd_parsl.workflow import DefaultStageTypes, Stage, Workflow, WorkflowExecutor


class SimpleWorkflowExecutor(WorkflowExecutor):
    def __init__(self, settings):
        super().__init__(settings)

    def setup_single_workflow(self, iteration):
        stage_order = [DefaultStageTypes.GEN, DefaultStageTypes.G4, DefaultStageTypes.DETSIM]

        workflow = Workflow(stage_order, self.fcls)
        s = Stage(DefaultStageTypes.DETSIM)
        s.run_dir = str(pathlib.Path(self.run_opts['output']) / str(iteration))

        # workflow will automatically fill in g4 and gen stages, with
        # run_dir inherited from detsim stage
        workflow.add_final_stage(s)

        return workflow


def main(settings):
    wfe = SimpleWorkflowExecutor(settings)
    wfe.execute()

if __name__ == '__main__':
    settings = {
        'run': {
            'output': 'output',
            'fclpath': 'fcls',
            'nsubruns': 10,
            'max_futures': 10
        },
        'larsoft': {},
        'fcls': {
            'gen': 'gen.fcl',
            'g4': 'g4.fcl',
            'detsim': 'detsim.fcl'
        },
        'queue': {},
        'workflow': {}
    }
    # or load from a JSON file

    main(settings)
```

Running the above code will print some dummy output showing the sequence of
commands required to generate the `DETSIM` stage:
```
lar -c gen.fcl  --output output/gen.root
lar -c g4.fcl -s output/gen.root --output output/g4.root
lar -c detsim.fcl -s output/g4.root --output output/detsim.root
```

### Adding Parsl

To submit work with Parsl, you need to assign a `runfunc` for at least the
final `Stage` added to the `Workflow`. Runfuncs for SBND and ICARUS data &
simulation workflows are provided within the `sbn_parsl.components` module, so
users may not need to implement these themsevles. The `runfunc` must have the
minimal function signature in the extended example below, and should return a
list of Parsl `File` objects. 

```python
...
from parsl.data_provider.files import File
from parsl.app.app import bash_app

@bash_app(cache=True)
def fcl_future(inputs=[], outputs=[]):
    """Return a shell script containing the commands to run LArSoft and produce
    the output file from the arguments."""
    my_bash_script = ...
    return my_bash_script

def runfunc(self, fcl, input_files, output_dir):
    output_file = File(...) # user specifies the filename
    future = fcl_future(outputs=[output_file])
    return future.outputs

...
class SimpleWorkflowExecutor(WorkflowExecutor):
    def __init__(self, settings):
        super().__init__(settings)

    def setup_workflow(self):
        stage_order = [StageType.GEN, StageType.G4, StageType.DETSIM]

        self.workflow = Workflow(stage_order, self.fcls)
        s = Stage(StageType.DETSIM)
        s.run_dir = self.run_opts['output']
>>>     s.runfunc = runfunc
...
```

Note that `runfunc` function takes `self` as the first argument, despite being
defined in the global scope. `sbnd_parsl` sets up the appropriate bindings at
runtime so that `runfunc` can access the member variables and functions of the
`Stage` it is bound to. This is useful to add logic based on the stage's type
or `.fcl` file.

### Passing extra arguments to `runfunc`

You can extend `runfunc` with arbitrary arguments while preserving the function
signature using `functools.partial`. Below, we modify `runfunc` from the
original example to pass the `WorkflowExecutor` object as an extra argument.
This allows us to access the user settings within `runfunc`:

```Python
import functools

def my_runfunc(self, fcl, input_files, output_dir, executor):
    output_filepath = executor.output_dir
    ...

class SimpleWorkflowExecutor(WorkflowExecutor):
    ...

    def setup_workflow(self):
        ...
>>>     s.runfunc = functools.partial(my_runfunc, executor=self) 
```
