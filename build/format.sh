#!/bin/bash

base_dir=$(dirname "$(realpath $0)")/..

find "${base_dir}" "${base_dir}/lib" \
    -maxdepth 1 -name '*.py' \
    | xargs pyfmt
