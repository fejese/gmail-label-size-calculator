#!/bin/bash

base_dir=$(dirname "$(realpath $0)")/..

cd "${base_dir}"
python_root=.python

pip3 install --target "${python_root}" --upgrade \
    aiofiles \
    click \
    google-api-python-client \
    google-auth-httplib2 \
    google-auth-oauthlib \
    oauth2client
