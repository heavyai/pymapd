#!/bin/bash

echo
echo "[conda build]"
conda install -q conda-build anaconda-client conda-verify --yes

echo
echo "[add channels]"
conda config --add channels conda-forge
conda config --add channels rapidsai

conda create -n omnisci-dev python=${PYTHON} \
thrift=0.11.0 \
numpydoc \
"pyarrow>=0.10.0,<0.15" \
sqlalchemy>=1.3 \
numpy>=1.16 \
pandas>=0.24 \
coverage \
flake8 \
pytest \
pytest-cov \
pytest-mock \
shapely \
numba \
cudf \
cudatoolkit


conda activate omnisci-dev

pip install -e .
conda list omnisci-dev
echo
exit 0
