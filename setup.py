import sys
import os
from codecs import open

from setuptools import setup
from setuptools.extension import Extension
try:
    from Cython.Build import cythonize
    import numpy as np
except ImportError:
    build_extensions = False
else:
    build_extensions = True

here = os.path.abspath(os.path.dirname(__file__))

# Get the long description from the README file
with open(os.path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

install_requires = ['six', 'thrift == 0.11.0', 'sqlalchemy', 'numpy', 'pandas',
                    'pyarrow == 0.10.0']

# Optional Requirements


doc_requires = ['sphinx', 'numpydoc', 'sphinx-rtd-theme']
test_requires = ['coverage', 'pytest == 3.3.1', 'pytest-mock']
dev_requires = doc_requires + test_requires
gpu_requires = ['cudf', 'libcudf']
complete_requires = dev_requires + gpu_requires

if sys.version_info.major == 2:
    test_requires.append("mock")


extra_requires = {
    'docs': doc_requires,
    'test': test_requires,
    'dev': dev_requires,
    'gpu': gpu_requires,
    'complete': complete_requires,
}

# ------------
# C Extensions
# ------------
if build_extensions and not sys.platform.startswith('win'):
    try:
        import pyarrow
    except ImportError as msg:
        print('Failed to import pyarrow: %s' % (msg))
        extensions = []
        extra_kwargs = dict()
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
        ]
        extra_kwargs = dict(ext_modules=cythonize(extensions))
else:
    extra_kwargs = dict()

setup(
    name='pymapd',
    description='A DB API 2 compatible client for OmniSci (formerly MapD).',
    long_description=long_description,
    url='https://github.com/omnisci/mapd-core',

    author='Tom Augspurger',
    author_email='taugspurger@continuum.io',

    license='Apache Software License',

    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: Database',
        'Topic :: Scientific/Engineering',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    packages=['pymapd', 'mapd'],
    use_scm_version=True,
    setup_requires=['setuptools_scm'],
    install_requires=install_requires,
    extras_require=extra_requires,
    **extra_kwargs
)
