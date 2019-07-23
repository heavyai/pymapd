import os
from codecs import open

from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))

# Get the long description from the README file
with open(os.path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

install_requires = ['thrift == 0.11.0',
                    'sqlalchemy >= 1.3',
                    'numpy >= 1.16',
                    'pandas >= 0.24',
                    'pyarrow >= 0.10.0,<0.14',
                    'packaging >= 19.0']

# Optional Requirements


doc_requires = ['sphinx', 'numpydoc', 'sphinx-rtd-theme']
test_requires = ['coverage', 'pytest', 'pytest-mock', 'shapely']
dev_requires = doc_requires + test_requires
gpu_requires = ['cudf', 'libcudf']
complete_requires = dev_requires + gpu_requires


extra_requires = {
    'docs': doc_requires,
    'test': test_requires,
    'dev': dev_requires,
    'gpu': gpu_requires,
    'complete': complete_requires,
}

setup(
    name='pymapd',
    description='A DB API 2 compatible client for OmniSci (formerly MapD).',
    long_description=long_description,
    url='https://github.com/omnisci/pymapd',

    author='OmniSci',
    author_email='community@omnisci.com',

    license='Apache Software License',

    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: Database',
        'Topic :: Scientific/Engineering',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3 :: Only',
    ],
    packages=['pymapd', 'omnisci'],
    use_scm_version=True,
    setup_requires=['setuptools_scm'],
    install_requires=install_requires,
    extras_require=extra_requires
)
