#!/bin/bash

if [ "$1" == "build" ]; then
  rm -Rf dist build && python3 setup.py sdist bdist_wheel
  twine check dist/*

  python3 make_lambda_build.py
elif [ "$1" == "deploy-test" ]; then
  twine upload --repository-url https://test.pypi.org/legacy/ dist/*
elif [ "$1" == "deploy-prod" ]; then
  twine upload --repository pypi dist/*
fi
