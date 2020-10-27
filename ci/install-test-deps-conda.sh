#!/bin/bash

echo
echo "[conda build]"
conda install -q conda-build anaconda-client conda-verify --yes

if [ "$CPU_ONLY" = true ] ; then
    ENV_FILE=./environment.yml
    ENV_NAME=omnisci-dev
else
    ENV_FILE=./environment_gpu.yml
    ENV_NAME=omnisci-gpu-dev
fi

# create a copy of the environment file, replacing
# with the python version we specify.
sed -E "s/- python[^[:alpha:]]+$/- python=$PYTHON/" ${ENV_FILE} > /tmp/${ENV_NAME}_${PYTHON}.yml

conda env create -f /tmp/${ENV_NAME}_${PYTHON}.yml

conda activate ${ENV_NAME}

pip install -e .
conda list ${ENV_NAME}
echo
exit 0
