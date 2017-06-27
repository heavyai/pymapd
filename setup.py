import sys
from os import path
from codecs import open

from setuptools import setup

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

install_requires = ['six', 'thrift']

if sys.version_info[0] == 2:
    install_requires.append("typing")

setup(
    name='pymapd',
    description='A python DB API 2 compatible client for mapd.',
    long_description=long_description,
    # url='https://github.com//',

    # author='The Python Packaging Authority',
    # author_email='pypa-dev@googlegroups.com',

    # license='MIT',

    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        # 'Topic :: Software Development :: Build Tools',
        # 'License :: OSI Approved :: MIT License',
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
    extras_require={
        'docs': ['sphinx', 'numpydoc', 'sphinx-rtd-theme'],
        'test': ['coverage', 'pytest'],
    },
)
