#!/bin/bash

echo
echo "[conda build]"
conda install -q conda-build anaconda-client conda-verify --yes


# create a copy of the environment file, replacing
# with the python version we specify.
sed -E "s/- python[^[:alpha:]]+$/- python=$PYTHON/" ./environment_gpu.yml > /tmp/environment_gpu_${PYTHON}.yml

conda env create -f /tmp/environment_gpu_${PYTHON}.yml

conda activate omnisci-gpu-dev

pip install -e .
conda list omnisci-dev
echo
exit 0
