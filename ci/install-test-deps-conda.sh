#!/bin/bash

echo
echo "[conda build]"
conda install -q conda-build anaconda-client conda-verify --yes

echo
echo "[add channels]"
conda config --add channels conda-forge
conda config --add channels rapidsai

conda create -n omnisci-dev python=${PYTHON} \
thrift=0.13.0 \
numpydoc \
"pyarrow>=0.14.0,<0.17" \
"sqlalchemy>=1.3" \
"numpy>=1.16" \
"pandas>=1.0,<2.0" \
coverage \
flake8 \
pytest \
pytest-cov \
pytest-mock \
shapely \
numba \
cudf \
cudatoolkit \
"rbc=0.2.2"

conda activate omnisci-dev

pip install -e .
conda list omnisci-dev
echo
exit 0
