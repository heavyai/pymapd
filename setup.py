import os
from codecs import open

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))

# Get the long description from the README file
with open(os.path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

install_requires = [
    'pyomnisci',
]

extra_requires = []

setup(
    name='pymapd',
    description='A wrapper for pyomnisci for backwards compatibility.',
    long_description=long_description,
    url='https://github.com/omnisci/pymapd',
    author='OmniSci',
    author_email='community@omnisci.com',
    license='Apache Software License',
    python_requires='>=3.7,<3.9',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: Database',
        'Topic :: Scientific/Engineering',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3 :: Only',
    ],
    packages=find_packages(),
    use_scm_version=True,
    setup_requires=['setuptools_scm'],
    install_requires=install_requires,
)
