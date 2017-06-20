#!/usr/bin/env bash
echo "Generating bindings to ${1-.}"
thrift -gen py -out ${1-.} ${MAPD_HOME}/mapd.thrift
mv ${1-.}/mapd/ttypes.py ${1-.}/mapd/ttypes-backup.py
echo "Fixing recursive structs"
python ${MAPD_HOME}/SampleCode/fix_recursive_structs.py ${1-.}/mapd/ttypes-backup.py ${1-.}/mapd/ttypes.py
rm ${1-.}/__init__.py
