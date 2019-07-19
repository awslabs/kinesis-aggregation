#!/bin/bash

rm -Rf dist build && python3 setup.py sdist bdist_wheel

if [ "$1" == "check" ]; then
  twine check dist/*
elif [ "$1" == "deploy" ]; then
  twine upload --repository pypi dist/*
fi
