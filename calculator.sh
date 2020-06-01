#!/bin/bash

base_dir=$(dirname "$(realpath $0)")

python_root=${base_dir}/.python

PATH="${python_root}"/bin:$PATH PYTHONPATH="${python_root}" \
    ${base_dir}/calculator.py \
    "$@"
