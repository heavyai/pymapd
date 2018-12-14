set -e

echo "Building pymapd"
conda build conda-recipes/pymapd -c conda-forge -c numba -c rapidsai -c nvidia -c defaults --python ${PYTHON}
