#!/bin/bash

# To be run from root of repo

pip install -r ci/requirements.txt

python setup.py sdist
pip install dist/*.tar.gz
