.. _contributing:

Contributing to pymapd
======================

As an open-source company, OmniSci welcomes contributions to all of its open-source repositories,
including pymapd. All discussion and development takes place via the `pymapd GitHub repository`_.

It is suggested, but not required, that you `create a GitHub issue`_ before contributing a feature or bug fix. This is so that other
developers 1) know that you are working on the feature/issue and 2) that internal OmniSci experts can help you navigate
any database-specific logic that may not be obvious within pymapd. All patches should be submitted as `pull requests`_, and upon passing
the test suite and review by OmniSci, will be merged to master for release as part of the next package release cycle.

-----------------------------
Development Environment Setup
-----------------------------

pymapd is written in plain Python 3 (i.e. no Cython), and as such, doesn't require any specialized development
environment outside of installing the dependencies. However, we do suggest using a separate conda environment or
virtualenv to ensure that your changes work without relying on unspecified system-level Python packages.

To set up a conda environment with python 3.7:

.. code-block:: shell

   # create a conda environment
   conda create -n pymapd_dev
   conda activate pymapd_dev

   # ipython is optional, other packages required to run test suite
   conda install python=3.7 ipython pytest pytest-mock shapely

   # get pymapd repo
   git clone https://github.com/omnisci/pymapd.git

   # install pymapd using pip...make sure you are in the pymapd folder cloned above
   # pip will install dependencies needed to run pymapd
   pip install -e .

At this point, you have everything you need to develop pymapd. However, to run the test suite, you need to be running
an instance of OmniSci on the same machine you are devloping on. OmniSci provides `Docker`_ images that work great for this purpose.

------------------------
Docker Environment Setup
------------------------

*********************
OmniSci Core CPU-only
*********************

Unless you are planning on developing GPU-specific functionality in pymapd, using the `CPU image`_ is enough to run the test suite:

.. code-block:: shell

   docker run \
     -d \
     --name omnisci \
     -p 6274:6274 \
     -p 6278:6278 \
     --ipc=host \
     -v /home/<username>/omnisci-storage:/omnisci-storage \
     omnisci/core-os-cpu

With the above code, we:
   * create/run an instance of OmniSci Core CPU as a daemon (i.e. running in the background until stopped)
   * forward ports ``6274`` (binary connection) and ``6278`` (http connection).
   * set ``ipc=host`` for testing shared memory/IPC functionality
   * point to a local directory to store data loaded to OmniSci. This allows our container to be ephemeral.

To run the test suite, call ``pytest`` from the top-level pymapd folder:

.. code-block:: shell

   (pymapd_dev) laptop:~/github_work/pymapd$ pytest

``pytest`` will run through the test suite, running the tests against the Docker container. Because we are using CPU-only, the
test suite skips the GPU tests, and you can expect to see the following messages at the end of the test suite run:

.. code-block:: shell

   =============================================== short test summary info ================================================
   SKIPPED [4] tests/test_data_no_nulls_gpu.py:15: No GPU available
   SKIPPED [1] tests/test_deallocate.py:34: No GPU available
   SKIPPED [1] tests/test_deallocate.py:54: deallocate non-functional in recent distros
   SKIPPED [1] tests/test_deallocate.py:67: No GPU available
   SKIPPED [1] tests/test_deallocate.py:80: deallocate non-functional in recent distros
   SKIPPED [1] tests/test_deallocate.py:92: No GPU available
   SKIPPED [1] tests/test_deallocate.py:105: deallocate non-functional in recent distros
   SKIPPED [2] tests/test_integration.py:207: No GPU available
   SKIPPED [1] tests/test_integration.py:238: No GPU available
   ================================== 69 passed, 13 skipped, 1 warnings in 19.40 seconds ==================================

************************
OmniSci Core GPU-enabled
************************

To run the pymapd test suite with the GPU tests, the workflow is pretty much the same as CPU-only, except with the `OmniSci Core
GPU-enabled`_ container:

.. code-block:: shell

   docker run \
     --runtime=nvidia \
     -d \
     --name omnisci \
     -p 6274:6274 \
     -p 6278:6278 \
     --ipc=host \
     -v /home/<username>/omnisci-storage:/omnisci-storage \
     omnisci/core-os-cuda

You also need to `install cudf`_ in your development environment. Because cudf is in active development, and requires attention
to the specific version of CUDA installed, we recommend checking the `cudf documentation`_ to get the most up-to-date
installation instructions.

-------------------------------
Updating Apache Thrift Bindings
-------------------------------

When the upstream `mapd-core`_ project updates its Apache Thrift definition file, the bindings shipped with
``pymapd`` need to be regenerated. From the root of the ``pymapd`` repository, run

.. code-block:: shell

   python scripts/generate_accelerated_bindings.py </path/to/mapd-core>/mapd.thrift


The python script above requires Apache Thrift is installed and on the PATH. Running the script will update
two files, ``mapd/MapD.py`` and ``mapd/ttypes.py``, which can be committed to the repository. When the bindings are
updated, take note to remove unneeded imports as shown in this `commit`_, as the unneeded imports can be problematic,
especially when calling pymapd from other languages (specifically, R).

--------------------------------
Publishing a new package version
--------------------------------

pymapd doesn't currently follow a rigid release schedule; rather, when enough functionality is deemed to be "enough" for a new
version to be released, or a sufficiently serious bug/issue is fixed, we will release a new version. pymapd is distributed via `PyPI`_
and `conda-forge`_.

Prior to submitting to PyPI and/or conda-forge, create a new `release tag`_ on GitHub (with notes), then run ``git pull`` to bring this tag to your
local pymapd repository folder.

****
PyPI
****

To publish to PyPI, we use the `twine`_ package via the CLI. twine only allows for submitting to PyPI by registered users
(currently, internal OmniSci employees):

.. code-block:: shell

   conda install twine
   python setup.py sdist
   twine upload dist/*

Publishing a package to PyPI is near instantaneous after runnning ``twine upload dist/*``. Before running ``twine upload``, be sure
the ``dist`` directory only has the current version of the package you are intending to upload.

***********
conda-forge
***********

The release process for conda-forge is triggered via creating a new version number on the pymapd GitHub repository. Given the
volume of packages released on conda-forge, it can take several hours for the bot to open a PR on pymapd-feedstock. There is
nothing that needs to be done to speed this up, just be patient.

When the conda-forge bot opens a PR on the pymapd-feedstock repo, one of the feedstock maintainers needs to validate the correctness
of the PR, check the accuracy of the package versions on the `meta.yaml`_ recipe file, and then merge once the CI tests pass.

.. _mapd-core: https://github.com/omnisci/mapd-core
.. _Docker: https://hub.docker.com/u/omnisci
.. _CPU image: https://hub.docker.com/r/omnisci/core-os-cpu
.. _OmniSci Core GPU-enabled: https://hub.docker.com/r/omnisci/core-os-cuda
.. _install cudf: https://github.com/rapidsai/cudf#installation
.. _cudf documentation: https://rapidsai.github.io/projects/cudf/en/latest/
.. _commit: https://github.com/omnisci/pymapd/commit/28441055959e62443954a9826f1f03d876a1cfdb
.. _pymapd GitHub repository: https://github.com/omnisci/pymapd
.. _create a GitHub issue: https://github.com/omnisci/pymapd/issues
.. _pull requests: https://github.com/omnisci/pymapd/pulls
.. _PyPI: https://pypi.org/project/pymapd/
.. _conda-forge: https://github.com/conda-forge/pymapd-feedstock
.. _release tag: https://github.com/omnisci/pymapd/releases
.. _twine: https://pypi.org/project/twine/
.. _meta.yaml: https://github.com/conda-forge/pymapd-feedstock/blob/master/recipe/meta.yaml
