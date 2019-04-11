#!/bin/bash

# If running as root, dont use `sudo`
if [[ $EUID == 0 ]]; then
   sudo_cmd=""
else
   sudo_cmd=sudo
fi

$sudo_cmd apt update
$sudo_cmd apt install -y wget flake8 git

echo "[install-travis]"

# install iniconda
MINICONDA_DIR="$HOME/miniconda3"
time wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh || exit 1
time bash miniconda.sh -b -p "$MINICONDA_DIR" || exit 1

echo "[link conda PATH]"
export PATH="$HOME/miniconda3/bin:$PATH"

echo "[show conda]"
which conda

echo
echo "[update conda]"
conda config --set always_yes true --set changeps1 false || exit 1
conda update -q conda

echo
echo "[conda build]"
conda install -q conda-build anaconda-client conda-verify --yes

echo
echo "[add channels]"
conda config --add channels conda-forge || exit 1

conda create -n omnisci-dev python=${PYTHON} \
thrift=0.11.0 \
numpydoc \
"pyarrow>=0.10.0,<0.14" \
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
-c rapidsai

conda activate omnisci-dev

pip install -e .
conda list omnisci-dev
echo
exit 0
