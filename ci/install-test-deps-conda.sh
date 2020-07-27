#!/bin/bash

echo
echo "[conda build]"
conda install -q conda-build anaconda-client conda-verify --yes


# create a copy of the environment file, replacing
# with the python version we specify.
sed -E "s/python.+$/python=$PYTHON/" ./environment.yml > /tmp/environment_${PYTHON}.yml

conda env create -f /tmp/environment_${PYTHON}.yml

conda activate omnisci-dev

pip install -e .
conda list omnisci-dev
echo
exit 0
