import shutil
import subprocess as sp


def prepare_task():
    sp.run(
        "make luigi-scheduler".split()
    )


def run_luigi_task(module_name, task_name, parameter='', value=''):
    if parameter == '':
        param_string = ''
    else:
        param_string = f'--{parameter} {value}'
    sp.run(
        f"luigi --module gomus.{module_name} "
        f"{task_name}ToDB {param_string}".split()
    )


def rename_output(name, offset):
    base_path = 'output/gomus/'
    shutil.move(base_path + name,
                base_path + f'{offset}_' + name)
