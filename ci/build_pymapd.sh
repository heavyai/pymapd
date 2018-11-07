set -e

echo "Building pymapd"
conda-verify conda build conda-recipes/pymapd -c conda-forge -c numba -c gpuopenanalytics/label/dev -c defaults --python ${PYTHON}
