#!/bin/bash

python_root=$(dirname "$(realpath $0)")/.python

pip3 install --target "${python_root}" --upgrade \
    google-api-python-client \
    google-auth-httplib2 \
    google-auth-oauthlib \
    oauth2client
