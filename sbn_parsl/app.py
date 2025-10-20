import argparse
import pathlib
import json

import parsl

from sbn_parsl.workflow import WorkflowExecutor
from sbn_parsl.utils import create_default_useropts, create_parsl_config
from sbn_parsl.dfk_hacks import apply_hacks


def entry_point(argv, wfe_class):
    parser = argparse.ArgumentParser(prog='sbn_parsl')
    parser.add_argument('settings', help='Path to JSON settings file') 
    parser.add_argument('-o', '--output-dir', help='Directory for outputs') 
    args = parser.parse_args()

    with open(args.settings, 'r') as f:
        settings = json.load(f)

    if args.output_dir is not None:
        settings['run']['output'] = args.output_dir

    user_opts = create_default_useropts()
    user_opts['run_dir'] = str(pathlib.Path(settings['run']['output']) / 'runinfo')
    user_opts.update(settings['queue'])
    parsl_config = create_parsl_config(user_opts)
    print(parsl_config)
    parsl.clear()

    with parsl.load(parsl_config) as dfk:
        apply_hacks(dfk)
        wfe = wfe_class(settings)
        wfe.execute()
