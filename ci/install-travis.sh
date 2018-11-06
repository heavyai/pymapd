#!/bin/bash

echo "[install-travis]"

# install iniconda
MINICONDA_DIR="$HOME/miniconda3"
time wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh || exit 1
time bash miniconda.sh -b -p "$MINICONDA_DIR" || exit 1

echo
echo "[show conda]"
which conda

echo
echo "[update conda]"
conda config --set always_yes true --set changeps1 false || exit 1
conda update -q conda

echo
echo "[conda build]"
conda install conda-build anaconda-client --yes

echo
echo "[add channels]"
conda config --add channels conda-forge || exit 1

conda env create -f environment.yml python=${PYTHON}
source activate omnisci-dev

#list of dev packages not needed for general conda environment.yml file
conda install -q \
      coverage \
      flake8 \
      pytest=3.3.1 \
      pytest-cov \
      pytest-mock \
      mock

pip install -e .
conda list omnisci-dev
exit 0
