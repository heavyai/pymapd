.. _contributing:

Contributing to pymapd
======================

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
   * set `ipc=host` for testing shared memory/IPC functionality
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

------------------------
Updating Thrift Bindings
------------------------

When the upstream `mapd-core`_ project updates its Thrift definition, we have to
regenerate the bindings we ship with ``pymapd``. From the root of the ``pymapd``
repository, run

.. code-block:: shell

   python scripts/generate_accelerated_bindings.py </path/to/mapd-core>/mapd.thrift


The requires that Thrift is installed and on your PATH. Running it will update
two files, ``mapd/MapD.py`` and ``mapd/ttypes.py``, which can be committed to
the repository.


.. _mapd-core: https://github.com/omnisci/mapd-core
.. _Docker: https://hub.docker.com/u/omnisci
.. _CPU image: https://hub.docker.com/r/omnisci/core-os-cpu
.. _OmniSci Core GPU-enabled: https://hub.docker.com/r/omnisci/core-os-cuda
.. _install cudf: https://github.com/rapidsai/cudf#installation
.. _cudf documentation: https://rapidsai.github.io/projects/cudf/en/latest/
