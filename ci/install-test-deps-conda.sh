#!/bin/bash

echo
echo "[conda build]"
conda install -q conda-build anaconda-client conda-verify --yes

echo
echo "[add channels]"
conda config --add channels conda-forge
conda config --add channels rapidsai
conda config --add channels nvidia

# ATM cudf uses pandas >=0.25,<0.26 pinning
conda create --yes -n omnisci-dev python=${PYTHON:-3.7} \
thrift=0.13.0 \
cudf=0.13 \
cudatoolkit=10.1 \
arrow-cpp=0.15.0 \
pyarrow==0.15.0 \
"pandas>=0.25,<0.26" \
"sqlalchemy>=1.3" \
"numpy>=1.16" \
numpydoc \
coverage \
flake8 \
pre-commit \
pytest-cov \
pytest-mock \
shapely \
sphinx  \
requests \
sphinx_rtd_theme \
rbc=0.2.2

conda activate omnisci-dev

pip install -e .
conda list omnisci-dev
echo
exit 0
