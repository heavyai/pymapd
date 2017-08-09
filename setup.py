import sys
import os
from codecs import open

from setuptools import setup
from setuptools.extension import Extension
from Cython.Build import cythonize
import numpy as np

here = os.path.abspath(os.path.dirname(__file__))

# Get the long description from the README file
with open(os.path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

install_requires = ['six', 'thrift']

if sys.version_info[0] == 2:
    install_requires.append("typing")

# Optional Requirements


doc_requires = ['sphinx', 'numpydoc', 'sphinx-rtd-theme']
test_requires = ['coverage', 'pytest', 'pytest-mock', 'sqlalchemy']
dev_requires = doc_requires + test_requires
gpu_requires = ['pygdf', 'libgdf']
arrow_requires = ['pyarrow']
complete_requires = dev_requires + gpu_requires + arrow_requires


extra_requires = {
    'docs': doc_requires,
    'test': test_requires,
    'dev': dev_requires,
    'gpu': gpu_requires,
    'arrow': arrow_requires,
    'complete': complete_requires,
}

# ------------
# C Extensions
# ------------
try:
    import pyarrow
except ImportError:
    extensions = []
else:
    home = os.path.dirname(pyarrow.__file__)
    include = os.path.join(home, 'include')
    link_args = []

    if sys.platform == "darwin":
        link_args.append('-Wl,-rpath,@loader_path/pyarrow')
    else:
        link_args.append("-Wl,-rpath,$ORIGIN/pyarrow")

    extensions = [
        Extension(
            "pymapd.shm",
            ["pymapd/shm.pyx"],
            libraries=['arrow', 'arrow_python'],
            include_dirs=[np.get_include(), include],
            extra_compile_args=['-std=c++11'],
            extra_link_args=['-std=c++11'],
            language="c++",
        ),
        # Extension(
        #     "pymapd.cpu",
        #     ["pymapd/cpu.pyx", "cpp/src/cpu.cpp"],
        #     include_dirs=[np.get_include()],
        #     language="c++",
        #     extra_compile_args=['-std=c++11'],
        #     extra_link_args=['-std=c++11'],
        # )
    ]

setup(
    name='pymapd',
    description='A python DB API 2 compatible client for mapd.',
    long_description=long_description,
    url='https://github.com/mapd/mapd-core',

    author='Tom Augspurger',
    author_email='taugspurger@continuum.io',

    license='Apache Software License',

    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        # 'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],

    # keywords='sample setuptools development',

    packages=['pymapd', 'mapd'],
    use_scm_version=True,

    setup_requires=['setuptools_scm'],
    install_requires=install_requires,
    extras_require=extra_requires,
    ext_modules=cythonize(extensions),
)
