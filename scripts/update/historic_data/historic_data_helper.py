"""Provides utilities for downloading historic data from Gomus."""

import os
import shutil
import subprocess as sp


def prepare_task():
    """Prepare environment for running luigi."""
    sp.run('make luigi-scheduler'.split())
    sp.run('mkdir historic_output/'.split())


def run_luigi_task(module_name, task_name, parameter='', value=''):
    """Run a luigi task via shell."""
    param_string = f'--{parameter} {value}' if parameter else ''
    sp.run(
        f'luigi --module gomus.{module_name} '
        f'{task_name}ToDb {param_string}'.split()
    )


def rename_output(name, offset):
    """Rename a file using the given offset."""
    base_path = f'{os.environ["OUTPUT_DIR"]}/gomus/'
    try:
        shutil.move(f'{base_path}{name}', f'historic_output/{offset}_{name}')
    except FileNotFoundError:
        print(f"Could not find '{name}'!")
