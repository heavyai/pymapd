#!/bin/bash

echo
echo "[conda build]"
conda install -q conda-build anaconda-client conda-verify --yes

echo
echo "[add channels]"
conda config --add channels conda-forge
conda config --add channels rapidsai
conda config --add channels nvidia

# libtiff pinning info:
# https://github.com/conda-forge/pillow-feedstock/issues/73
conda create -n omnisci-dev python=${PYTHON} \
'thrift=0.13.0' \
'cudf=0.13' \
'cudatoolkit=10.1' \
'arrow-cpp=0.15.0' \
'pyarrow==0.15.0' \
'pandas>=0.25,<0.26' \
libtiff=4.0.10 \
geopandas \
shapely \
sqlalchemy \
numpy \
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
'rbc==0.2.2'

conda activate omnisci-dev

pip install -e .
conda list omnisci-dev
echo
exit 0
