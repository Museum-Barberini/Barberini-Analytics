import os
import shutil
import subprocess as sp


def prepare_task():
    sp.run(
        "make luigi-scheduler".split()
    )
    sp.run(
        "mkdir historic_output/".split()
    )
    sp.run('make luigi-scheduler'.split())
    sp.run('mkdir historic_output/'.split())


def run_luigi_task(module_name, task_name, parameter='', value=''):
    if parameter == '':
        param_string = ''
    else:
        param_string = f'--{parameter} {value}'
    param_string = f'--{parameter} {value}' if parameter else ''
    sp.run(
        f'luigi --module gomus.{module_name} '
        f'{task_name}ToDb {param_string}'.split()
    )


def rename_output(name, offset):
    base_path = f'{os.environ["OUTPUT_DIR"]}/gomus/'
    try:
        shutil.move(f'{base_path}{name}', f'historic_output/{offset}_{name}')
    except FileNotFoundError:
        print(f"Could not find '{name}'!")
